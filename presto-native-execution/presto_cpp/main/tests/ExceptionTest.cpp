/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <gtest/gtest.h>
#include "presto_cpp/main/common/Exception.h"
#include "velox/common/base/VeloxException.h"

using namespace facebook::presto;

class ExceptionTest : public ::testing::Test {
 protected:
  VeloxToPrestoExceptionTranslator translator_;
};

// =============================================================================
// Tests for TRY-catchable errors (catchableByTry = true)
// =============================================================================

TEST_F(ExceptionTest, arithmeticErrorIsCatchableByTry) {
  // kArithmeticError maps to DIVISION_BY_ZERO which should be catchable by TRY
  auto veloxException = velox::VeloxUserError(
      __FILE__,
      __LINE__,
      __FUNCTION__,
      "test",
      "division by zero",
      velox::error_source::kErrorSourceUser,
      velox::error_code::kArithmeticError,
      false);

  auto result = translator_.translate(veloxException);

  EXPECT_EQ(result.errorCode.code, 0x00000008);
  EXPECT_EQ(result.errorCode.name, "DIVISION_BY_ZERO");
  EXPECT_EQ(result.errorCode.type, protocol::ErrorType::USER_ERROR);
  EXPECT_FALSE(result.errorCode.retriable);
  EXPECT_TRUE(result.errorCode.catchableByTry);
}

TEST_F(ExceptionTest, invalidArgumentErrorIsCatchableByTry) {
  // kInvalidArgument maps to INVALID_FUNCTION_ARGUMENT which should be
  // catchable by TRY
  auto veloxException = velox::VeloxUserError(
      __FILE__,
      __LINE__,
      __FUNCTION__,
      "test",
      "invalid argument",
      velox::error_source::kErrorSourceUser,
      velox::error_code::kInvalidArgument,
      false);

  auto result = translator_.translate(veloxException);

  EXPECT_EQ(result.errorCode.code, 0x00000007);
  EXPECT_EQ(result.errorCode.name, "INVALID_FUNCTION_ARGUMENT");
  EXPECT_EQ(result.errorCode.type, protocol::ErrorType::USER_ERROR);
  EXPECT_FALSE(result.errorCode.retriable);
  EXPECT_TRUE(result.errorCode.catchableByTry);
}

// =============================================================================
// Tests for non-TRY-catchable user errors (catchableByTry = false)
// =============================================================================

TEST_F(ExceptionTest, unsupportedErrorIsNotCatchableByTry) {
  // kUnsupported is NOT catchable by TRY - it indicates a feature not supported
  auto veloxException = velox::VeloxUserError(
      __FILE__,
      __LINE__,
      __FUNCTION__,
      "test",
      "operation not supported",
      velox::error_source::kErrorSourceUser,
      velox::error_code::kUnsupported,
      false);

  auto result = translator_.translate(veloxException);

  EXPECT_EQ(result.errorCode.name, "NOT_SUPPORTED");
  EXPECT_EQ(result.errorCode.type, protocol::ErrorType::USER_ERROR);
  EXPECT_FALSE(result.errorCode.catchableByTry);
}

TEST_F(ExceptionTest, unsupportedInputUncatchableIsNotCatchableByTry) {
  // kUnsupportedInputUncatchable explicitly indicates an uncatchable error
  auto veloxException = velox::VeloxUserError(
      __FILE__,
      __LINE__,
      __FUNCTION__,
      "test",
      "unsupported input",
      velox::error_source::kErrorSourceUser,
      velox::error_code::kUnsupportedInputUncatchable,
      false);

  auto result = translator_.translate(veloxException);

  EXPECT_EQ(result.errorCode.name, "NOT_SUPPORTED");
  EXPECT_FALSE(result.errorCode.catchableByTry);
}

TEST_F(ExceptionTest, schemaMismatchIsNotCatchableByTry) {
  // kSchemaMismatch is NOT catchable - schema errors should fail the query
  auto veloxException = velox::VeloxUserError(
      __FILE__,
      __LINE__,
      __FUNCTION__,
      "test",
      "schema mismatch",
      velox::error_source::kErrorSourceUser,
      velox::error_code::kSchemaMismatch,
      false);

  auto result = translator_.translate(veloxException);

  EXPECT_EQ(result.errorCode.name, "GENERIC_USER_ERROR");
  EXPECT_FALSE(result.errorCode.catchableByTry);
}

// =============================================================================
// Tests for runtime/system errors (catchableByTry = false)
// =============================================================================

TEST_F(ExceptionTest, memoryExceededIsNotCatchableByTry) {
  // Memory errors are infrastructure errors and should NOT be catchable
  auto veloxException = velox::VeloxRuntimeError(
      __FILE__,
      __LINE__,
      __FUNCTION__,
      "test",
      "memory limit exceeded",
      velox::error_source::kErrorSourceRuntime,
      velox::error_code::kMemCapExceeded,
      false);

  auto result = translator_.translate(veloxException);

  EXPECT_EQ(result.errorCode.name, "EXCEEDED_LOCAL_MEMORY_LIMIT");
  EXPECT_EQ(result.errorCode.type, protocol::ErrorType::INSUFFICIENT_RESOURCES);
  EXPECT_FALSE(result.errorCode.catchableByTry);
}

TEST_F(ExceptionTest, internalErrorIsNotCatchableByTry) {
  // Internal errors should NOT be catchable
  auto veloxException = velox::VeloxRuntimeError(
      __FILE__,
      __LINE__,
      __FUNCTION__,
      "test",
      "internal error",
      velox::error_source::kErrorSourceRuntime,
      velox::error_code::kInvalidState,
      false);

  auto result = translator_.translate(veloxException);

  EXPECT_EQ(result.errorCode.name, "GENERIC_INTERNAL_ERROR");
  EXPECT_EQ(result.errorCode.type, protocol::ErrorType::INTERNAL_ERROR);
  EXPECT_FALSE(result.errorCode.catchableByTry);
}

// =============================================================================
// Tests for fallback errors (catchableByTry = false)
// =============================================================================

TEST_F(ExceptionTest, unknownErrorFallsBackToInternalErrorNotCatchable) {
  // Unknown error codes should fallback to GENERIC_INTERNAL_ERROR
  // which is NOT catchable
  auto veloxException = velox::VeloxRuntimeError(
      __FILE__,
      __LINE__,
      __FUNCTION__,
      "test",
      "unknown error",
      "unknown_source",
      "unknown_code",
      false);

  auto result = translator_.translate(veloxException);

  EXPECT_EQ(result.errorCode.code, 0x00010000);
  EXPECT_EQ(result.errorCode.name, "GENERIC_INTERNAL_ERROR");
  EXPECT_EQ(result.errorCode.type, protocol::ErrorType::INTERNAL_ERROR);
  EXPECT_FALSE(result.errorCode.catchableByTry);
}

TEST_F(ExceptionTest, stdExceptionIsNotCatchableByTry) {
  // std::exception should translate to GENERIC_INTERNAL_ERROR
  // which is NOT catchable
  std::runtime_error e("test error");

  auto result = translator_.translate(e);

  EXPECT_EQ(result.errorCode.code, 0x00010000);
  EXPECT_EQ(result.errorCode.name, "GENERIC_INTERNAL_ERROR");
  EXPECT_EQ(result.errorCode.type, protocol::ErrorType::INTERNAL_ERROR);
  EXPECT_FALSE(result.errorCode.catchableByTry);
  EXPECT_EQ(result.type, "std::exception");
}

// =============================================================================
// Tests to verify error map contents
// =============================================================================

TEST_F(ExceptionTest, errorMapContainsCatchableErrors) {
  const auto& errorMap = translator_.testingErrorMap();

  // Verify user error source exists
  auto userErrors = errorMap.find(velox::error_source::kErrorSourceUser);
  ASSERT_NE(userErrors, errorMap.end());

  // Verify kArithmeticError is catchable
  auto arithmeticError =
      userErrors->second.find(velox::error_code::kArithmeticError);
  ASSERT_NE(arithmeticError, userErrors->second.end());
  EXPECT_TRUE(arithmeticError->second.catchableByTry);

  // Verify kInvalidArgument is catchable
  auto invalidArgError =
      userErrors->second.find(velox::error_code::kInvalidArgument);
  ASSERT_NE(invalidArgError, userErrors->second.end());
  EXPECT_TRUE(invalidArgError->second.catchableByTry);
}

TEST_F(ExceptionTest, errorMapContainsNonCatchableErrors) {
  const auto& errorMap = translator_.testingErrorMap();

  // Verify user error source exists
  auto userErrors = errorMap.find(velox::error_source::kErrorSourceUser);
  ASSERT_NE(userErrors, errorMap.end());

  // Verify kUnsupported is NOT catchable
  auto unsupportedError =
      userErrors->second.find(velox::error_code::kUnsupported);
  ASSERT_NE(unsupportedError, userErrors->second.end());
  EXPECT_FALSE(unsupportedError->second.catchableByTry);

  // Verify kSchemaMismatch is NOT catchable
  auto schemaMismatchError =
      userErrors->second.find(velox::error_code::kSchemaMismatch);
  ASSERT_NE(schemaMismatchError, userErrors->second.end());
  EXPECT_FALSE(schemaMismatchError->second.catchableByTry);
}
