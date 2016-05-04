// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "uda-sample.h"
#include <assert.h>
#include <sstream>
#include <math.h>
#include <impala_udf/udf.h>


using namespace impala_udf;
using namespace std;

template <typename T>
StringVal ToStringVal(FunctionContext* context, const T& val) {
  stringstream ss;
  ss << val;
  string str = ss.str();
  StringVal string_val(context, str.size());
  memcpy(string_val.ptr, str.c_str(), str.size());
  return string_val;
}

template <>
StringVal ToStringVal<DoubleVal>(FunctionContext* context, const DoubleVal& val) {
  if (val.is_null) return StringVal::null();
  return ToStringVal(context, val.val);
}

// This function initializes StringVal 'dst' with a newly allocated buffer of
// 'buf_len' bytes. The new buffer will be filled with zero. If allocation fails,
// 'dst' will be set to a null string. This allows execution to continue until the
// next time GetQueryStatus() is called (see IMPALA-2756).

static const int k = 6;
static int total = 0;

struct Node {
        int val;
        Node* next;
};

struct linkedList {
        Node* head;
        int len;
        linkedList() { len = 0; head = NULL; }

        void insert (int val, FunctionContext* ctx) {
                len++;

		StringVal* dst;
		dst->is_null = false;
                dst->len = sizeof(Node);
                dst->ptr = ctx->Allocate(dst->len);
                memset(dst->ptr, 0, dst->len);

                Node* nn = reinterpret_cast<Node*>(dst->ptr);// (Node*) malloc(sizeof(Node));
                nn->val = val;
                nn->next = NULL;
                if (head == NULL) head = nn;
                else {
                        nn->next = head;
                        head = nn;
                }
        }

        void sort () {
                if (len == 1) return;
                Node* temp = head;
                while (temp->next != NULL) {
                        Node* second = temp->next;
                        Node* min = temp;
                        while (second != NULL) {
				if (min->val > second->val) {
                                        min = second;
                                }
                                second = second->next;
                        }
                        int tempVal = min->val;
                        min->val = temp->val;
                        temp->val = tempVal;
                        temp = temp->next;
                }
        }

	void pop() {
                if (head == NULL) return;
                len--;
                Node* temp = head;
                head = head->next;
                free(temp);
        }

        int yieldPop() {
                if (head == NULL) return -1;
                len--;
                Node* temp = head;
                head = head->next;
                int ret = temp->val;
                free(temp);
                return ret;
        }

        void extend(linkedList* _list) {
		len+= _list->len;
                if (head == NULL) {
                        head = _list->head;
                        return;
                }
                Node* _head = head;
                while (_head->next != NULL) _head = _head->next;
                _head->next = _list->head;
        }
};

struct cNode {
        linkedList val;
        cNode* next;
};

struct compactor {
        cNode* head;
        int len;
	int H;
	int maxSize;
        compactor() { H = 0; maxSize = 0; len = 0; head = NULL; }

        void insert(int val, FunctionContext* ctx) {
		total++;
                if (head == NULL) grow(ctx);
		head->val.insert(val, ctx);
        }

        int length(int compactorNumber) {
                cNode* _head = head;
                while (compactorNumber > 1) {
                        compactorNumber--;
                        _head = _head->next;
                }
                return _head->val.len;
        }

        int size() {
                int Size = 0;
                cNode* _head = head;
                while (_head != NULL) {
                        Size += _head->val.len;
                        _head = _head->next;
                }
                return Size;
        }


	void grow(FunctionContext* ctx) {
                len++;
		
		StringVal* dst;
		dst->is_null = false;
 		dst->len = sizeof(cNode);
 		dst->ptr = ctx->Allocate(dst->len);
 		memset(dst->ptr, 0, dst->len);
                
		cNode* node = reinterpret_cast<cNode*>(dst->ptr); //(cNode*) malloc(sizeof(cNode));
                node->next = NULL;
                if (head == NULL) {
                        head = node;
                }
                else {
                        cNode* _head = head;
                        while (_head->next != NULL) _head = _head->next;
                        _head->next = node;
                }
                ++H;
                int sum = 0;
                for (int h = 1; h <= H; h++) {
                        int iVal = upperBound(h);
                        sum += (iVal +1);
                }
                maxSize = sum;
        }

        int upperBound (int h) {
                double dVal = 2; dVal/= 3; dVal = pow(dVal, H-h);
                dVal*= k;
                return (ceil(dVal));
        }
	
	void compact(FunctionContext* ctx) {
                while (size() >= maxSize) {
                        for (int h = 1; h <= H; h++) {
                                if (length(h) >= (upperBound(h) +1)) {
                                        if ((h+1) > H) grow(ctx);
                                        extend(h+1,COMPACT(h, ctx));
                                        if (size() < maxSize) break;
                                }
                        }
                }
        }

	linkedList* getCompactor(int compactorNumber) {
		cNode* _head = head;
                while (compactorNumber > 1) {
                        compactorNumber--;
                        _head = _head->next;
                }
		return (&_head->val);
	}

        linkedList* COMPACT(int compactorNumber, FunctionContext* ctx) {
                cNode* _head = head;
                while (compactorNumber > 1) {
                        compactorNumber--;
                        _head = _head->next;
                }
                _head->val.sort();
                srand(time(NULL));
                int random = rand()%2;

		StringVal* dst;
		dst->is_null = false;
                dst->len = sizeof(linkedList);
                dst->ptr = ctx->Allocate(dst->len);
                memset(dst->ptr, 0, dst->len);

                linkedList* _list = reinterpret_cast<linkedList*>(dst->ptr); //(linkedList*) malloc(sizeof(linkedList));
                if (random == 0) {
                        while (_head->val.len >= 2) {
                                _head->val.pop();
                                _list->insert(_head->val.yieldPop(), ctx);
                        }
                }
                else {
                        while (_head->val.len >= 2) {
                                _list->insert(_head->val.yieldPop(), ctx);
                                _head->val.pop();
                        }
                }
                return _list;
        }

	void extend (int compactorNumber, linkedList* _list) {
                cNode* _head = head;
                while (compactorNumber > 1) {
                        compactorNumber--;
                        _head = _head->next;
                }
                _head->val.extend(_list);
                _list->head = NULL;
        }
	
	int rank (int x) {
		int r = 0;
		cNode* _head = head;
		for (int h = 1; h <= H; h++) {
			Node* first = _head->val.head;
			while (first != NULL) {
				if (first->val <= x) r += pow(2,h-1);
				first = first->next;
			}
			_head = _head->next;
		}
		r = (int) (r*100/total);
		return r;
	}
};

void MedianInit(FunctionContext* ctx, StringVal* dst) {
  dst->is_null = false;
  dst->len = sizeof(compactor);
  dst->ptr = ctx->Allocate(dst->len);
  memset(dst->ptr, 0, dst->len);
}

void MedianUpdate(FunctionContext* ctx, const DoubleVal& src, StringVal* dst) {
  if (src.is_null) return;
  compactor* state = reinterpret_cast<compactor*>(dst->ptr);
  state->insert(src.val, ctx);
  state->compact(ctx);
}

void MedianMerge(FunctionContext* ctx, const StringVal& src, StringVal* dst) {
/*  compactor* src_state;
  compactor* dst_state;

  if (dst_state->H >= src_state->H) {
  	src_state = reinterpret_cast<compactor*>(src.ptr);
  	dst_state = reinterpret_cast<compactor*>(dst->ptr);
  }
  else {
	dst_state = reinterpret_cast<compactor*>(src.ptr);
	src_state = reinterpret_cast<compactor*>(dst->ptr);
  }

  for (int h = 1; h <= src_state->H; h++) {
	dst_state->extend(h,src_state->getCompactor(h));
  }
  dst_state->compact();
*/}

StringVal MedianFinalize(FunctionContext* ctx, const StringVal& src) {
  compactor state = *reinterpret_cast<compactor*>(src.ptr);
  ctx->Free(src.ptr);
  if (total == 0) return StringVal::null();
  
  int median = 100;
  int medianVal = 100;
  return ToStringVal(ctx, medianVal);
  cNode* _head = state.head;
  while (_head != NULL) {
	Node* first = _head->val.head;
	while (first != NULL) {
		int retVal = state.rank(first->val);
		retVal -= 50;
		retVal = abs(retVal);
		if (retVal < median) {
			median = retVal;
			medianVal = first->val;
		}
		first = first->next;
	}
	_head = _head->next;
  }
  return ToStringVal(ctx, medianVal); 
}

// ===========================================
// =========== CORRELATION ===================
// ===========================================

struct CorrState {
  // Sum (xy)
  double prod;
  // Sum(x)
  double sumx;
  // Sum(y)
  double sumy;
  // Sum(x) squared
  double sum_squaredx;
  // Sum(y) squared
  double sum_squaredy;
  // Total number of enteries
  int64_t count;
};

void CorrInit(FunctionContext* ctx, StringVal* dst) {
  dst->is_null = false;
  dst->len = sizeof(CorrState);
  dst->ptr = ctx->Allocate(dst->len);
  memset(dst->ptr, 0, dst->len);
}

void CorrUpdate(FunctionContext* ctx, const DoubleVal& src1, const DoubleVal& src2, 
		StringVal* dst) {
  if (src1.is_null || src2.is_null) return;
  CorrState* state = reinterpret_cast<CorrState*>(dst->ptr);
  state->sumx += src1.val;
  state->sumy += src2.val;

  state->sum_squaredx += src1.val * src1.val;
  state->sum_squaredy += src2.val * src2.val;

  state->prod += src1.val * src2.val; 
  ++state->count;
}

void CorrMerge(FunctionContext* ctx, const StringVal& src, StringVal* dst) {
  CorrState* src_state = reinterpret_cast<CorrState*>(src.ptr);
  CorrState* dst_state = reinterpret_cast<CorrState*>(dst->ptr);

  dst_state->sumx += src_state->sumx;
  dst_state->sumy += src_state->sumy;

  dst_state->sum_squaredx += src_state->sum_squaredx;
  dst_state->sum_squaredy += src_state->sum_squaredy;

  dst_state->prod += src_state->prod;
  dst_state->count += src_state->count;

}

const StringVal CorrSerialize(FunctionContext* ctx, const StringVal& src) {
  StringVal result(ctx, src.len);
  memcpy(result.ptr, src.ptr, src.len);
  ctx->Free(src.ptr);
  return result;
}

StringVal CorrFinalize(FunctionContext* ctx, const StringVal& src) {
  CorrState state = *reinterpret_cast<CorrState*>(src.ptr);
  ctx->Free(src.ptr); 
  if (state.count == 0 || state.count == 1) return StringVal::null();
  // Calculating correlation coefficient using Pearson Product Moment Correlation formula
  double corr = ((state.prod*state.count) - (state.sumx*state.sumy)) / (pow((((state.count*state.sum_squaredx) - (state.sumx*state.sumx)) * ((state.count*state.sum_squaredy) - (state.sumy*state.sumy))), 0.5));
  return ToStringVal(ctx, corr);
}

// ===========================================
// ============= COVAR_POP ===================
// ===========================================
//

StringVal CovarFinalize(FunctionContext* ctx, const StringVal& src) {
  CorrState state = *reinterpret_cast<CorrState*>(src.ptr);
  ctx->Free(src.ptr);
  if (state.count == 0 || state.count == 1) return StringVal::null();
  double covar = ((state.prod*state.count) - (state.sumx*state.sumy)) / (state.count * (state.count - 1));
  return ToStringVal(ctx, covar);
}

// ===========================================
// ============= REGR_SLOPE ==================
// ===========================================
// 

StringVal Regr_SlopeFinalize(FunctionContext* ctx, const StringVal& src) {
  CorrState state = *reinterpret_cast<CorrState*>(src.ptr);
  ctx->Free(src.ptr);
  if (state.count == 0 || state.count == 1) return StringVal::null();
  double regr_slope = ((state.prod*state.count) - (state.sumx*state.sumy)) / ((state.count*state.sum_squaredx) - (state.sumx*state.sumx));
  return ToStringVal(ctx, regr_slope);
}

// ===========================================
// ============= REGR_INTERCEPT ==============
// ===========================================
// 

StringVal Regr_InterceptFinalize(FunctionContext* ctx, const StringVal& src) {
  CorrState state = *reinterpret_cast<CorrState*>(src.ptr);
  ctx->Free(src.ptr);
  if (state.count == 0 || state.count == 1) return StringVal::null();
  double regr_intercept = (state.sumy - (((state.prod*state.count) - (state.sumx*state.sumy)) / ((state.count*state.sum_squaredx) - (state.sumx*state.sumx)))*state.sumx) / state.count;
  return ToStringVal(ctx, regr_intercept);
}

// ===========================================
// ================= REGR_R2 =================
// ===========================================
// 

StringVal Regr_R2Finalize(FunctionContext* ctx, const StringVal& src) {
  CorrState state = *reinterpret_cast<CorrState*>(src.ptr);
  ctx->Free(src.ptr);
  double vary = (state.sum_squaredy/state.count) - (pow((state.sumy/state.count),2));
  if (vary == 0 || state.count == 0 || state.count == 1) return StringVal::null();

  double varx = (state.sum_squaredx/state.count) - (pow((state.sumx/state.count),2));
  if (varx == 0) return ToStringVal(ctx, 1);

  // Calculating correlation coefficient using Pearson Product Moment Correlation formula
  double corr = ((state.prod*state.count) - (state.sumx*state.sumy)) / (pow((((state.count*state.sum_squaredx) - (state.sumx*state.sumx)) * ((state.count*state.sum_squaredy) - (state.sumy*state.sumy))), 0.5));
  return ToStringVal(ctx, corr*corr);
}



// =================================================
// ============ sum of squares uda =================
// =================================================


void SumOfSquaresInit(FunctionContext* context, BigIntVal* val) {
  val->is_null = false;
  val->val = 0;
}
void SumOfSquaresInit(FunctionContext* context, DoubleVal* val) {
  val->is_null = false;
  val->val = 0.0;
}

void SumOfSquaresUpdate(FunctionContext* context, const BigIntVal& input, BigIntVal* val) {
  if (input.is_null) return;
  val->val += input.val * input.val;
}
void SumOfSquaresUpdate(FunctionContext* context, const DoubleVal& input, DoubleVal* val) {
  if (input.is_null) return;
  val->val += input.val * input.val;
}

void SumOfSquaresMerge(FunctionContext* context, const BigIntVal& src, BigIntVal* dst) {
  dst->val += src.val;
}
void SumOfSquaresMerge(FunctionContext* context, const DoubleVal& src, DoubleVal* dst) {
  dst->val += src.val;
}

BigIntVal SumOfSquaresFinalize(FunctionContext* context, const BigIntVal& val) {
  return val;
}
DoubleVal SumOfSquaresFinalize(FunctionContext* context, const DoubleVal& val) {
  return val;
}

// ==========================================================================
// This is a sample of implementing SUM GROUP BY aggregate function.
// ==========================================================================

void SumGBInit(FunctionContext* context, BigIntVal* val) {
  val->is_null = false;
  val->val = 0;
}
void SumGBInit(FunctionContext* context, DoubleVal* val) {
  val->is_null = false;
  val->val = 0.0;
}

void SumGBUpdate(FunctionContext* context, const BigIntVal& input, BigIntVal* val) {
  if (input.is_null) return;
  val->val += input.val;
}
void SumGBUpdate(FunctionContext* context, const DoubleVal& input, DoubleVal* val) {
  if (input.is_null) return;
  val->val += input.val;
}

void SumGBMerge(FunctionContext* context, const BigIntVal& src, BigIntVal* dst) {
  dst->val += src.val;
}
void SumGBMerge(FunctionContext* context, const DoubleVal& src, DoubleVal* dst) {
  dst->val += src.val;
}

BigIntVal SumGBFinalize(FunctionContext* context, const BigIntVal& val) {
  return val;
}
DoubleVal SumGBFinalize(FunctionContext* context, const DoubleVal& val) {
  return val;
}

// ===============================================================================
// ===============================================================================


// ---------------------------------------------------------------------------
// This is a sample of implementing a COUNT aggregate function.
// ---------------------------------------------------------------------------
void CountInit(FunctionContext* context, BigIntVal* val) {
  val->is_null = false;
  val->val = 0;
}

void CountUpdate(FunctionContext* context, const IntVal& input, BigIntVal* val) {
  if (input.is_null) return;
  ++val->val;
}

void CountMerge(FunctionContext* context, const BigIntVal& src, BigIntVal* dst) {
  dst->val += src.val;
}

BigIntVal CountFinalize(FunctionContext* context, const BigIntVal& val) {
  return val;
}

// ---------------------------------------------------------------------------
// This is a sample of implementing a AVG aggregate function.
// ---------------------------------------------------------------------------
struct AvgStruct {
  double sum;
  int64_t count;
};

// Initialize the StringVal intermediate to a zero'd AvgStruct
void AvgInit(FunctionContext* context, StringVal* val) {
  val->is_null = false;
  val->len = sizeof(AvgStruct);
  val->ptr = context->Allocate(val->len);
  memset(val->ptr, 0, val->len);
}

void AvgUpdate(FunctionContext* context, const DoubleVal& input, StringVal* val) {
  if (input.is_null) return;
  assert(!val->is_null);
  assert(val->len == sizeof(AvgStruct));
  AvgStruct* avg = reinterpret_cast<AvgStruct*>(val->ptr);
  avg->sum += input.val;
  ++avg->count;
}

void AvgMerge(FunctionContext* context, const StringVal& src, StringVal* dst) {
  if (src.is_null) return;
  const AvgStruct* src_avg = reinterpret_cast<const AvgStruct*>(src.ptr);
  AvgStruct* dst_avg = reinterpret_cast<AvgStruct*>(dst->ptr);
  dst_avg->sum += src_avg->sum;
  dst_avg->count += src_avg->count;
}

// A serialize function is necesary to free the intermediate state allocation. We use the
// StringVal constructor to allocate memory owned by Impala, copy the intermediate state,
// and free the original allocation. Note that memory allocated by the StringVal ctor is
// not necessarily persisted across UDA function calls, which is why we don't use it in
// AvgInit().
const StringVal AvgSerialize(FunctionContext* context, const StringVal& val) {
  assert(!val.is_null);
  StringVal result(context, val.len);
  memcpy(result.ptr, val.ptr, val.len);
  context->Free(val.ptr);
  return result;
}

StringVal AvgFinalize(FunctionContext* context, const StringVal& val) {
  assert(!val.is_null);
  assert(val.len == sizeof(AvgStruct));
  AvgStruct* avg = reinterpret_cast<AvgStruct*>(val.ptr);
  StringVal result;
  if (avg->count == 0) {
    result = StringVal::null();
  } else {
    // Copies the result to memory owned by Impala
    result = ToStringVal(context, avg->sum / avg->count);
  }
  context->Free(val.ptr);
  return result;
}

// ---------------------------------------------------------------------------
// This is a sample of implementing the STRING_CONCAT aggregate function.
// Example: select string_concat(string_col, ",") from table
// ---------------------------------------------------------------------------
// Delimiter to use if the separator is NULL.
static const StringVal DEFAULT_STRING_CONCAT_DELIM((uint8_t*)", ", 2);

void StringConcatInit(FunctionContext* context, StringVal* val) {
  val->is_null = true;
}

void StringConcatUpdate(FunctionContext* context, const StringVal& str,
    const StringVal& separator, StringVal* result) {
  if (str.is_null) return;
  if (result->is_null) {
    // This is the first string, simply set the result to be the value.
    uint8_t* copy = context->Allocate(str.len);
    memcpy(copy, str.ptr, str.len);
    *result = StringVal(copy, str.len);
    return;
  }

  const StringVal* sep_ptr = separator.is_null ? &DEFAULT_STRING_CONCAT_DELIM :
      &separator;

  // We need to grow the result buffer and then append the new string and
  // separator.
  int new_size = result->len + sep_ptr->len + str.len;
  result->ptr = context->Reallocate(result->ptr, new_size);
  memcpy(result->ptr + result->len, sep_ptr->ptr, sep_ptr->len);
  result->len += sep_ptr->len;
  memcpy(result->ptr + result->len, str.ptr, str.len);
  result->len += str.len;
}

void StringConcatMerge(FunctionContext* context, const StringVal& src, StringVal* dst) {
  if (src.is_null) return;
  StringConcatUpdate(context, src, ",", dst);
}

// A serialize function is necesary to free the intermediate state allocation. We use the
// StringVal constructor to allocate memory owned by Impala, copy the intermediate
// StringVal, and free the intermediate's memory. Note that memory allocated by the
// StringVal ctor is not necessarily persisted across UDA function calls, which is why we
// don't use it in StringConcatUpdate().
const StringVal StringConcatSerialize(FunctionContext* context, const StringVal& val) {
  if (val.is_null) return val;
  StringVal result(context, val.len);
  memcpy(result.ptr, val.ptr, val.len);
  context->Free(val.ptr);
  return result;
}

// Same as StringConcatSerialize().
StringVal StringConcatFinalize(FunctionContext* context, const StringVal& val) {
  if (val.is_null) return val;
  StringVal result(context, val.len);
  memcpy(result.ptr, val.ptr, val.len);
  context->Free(val.ptr);
  return result;
}
