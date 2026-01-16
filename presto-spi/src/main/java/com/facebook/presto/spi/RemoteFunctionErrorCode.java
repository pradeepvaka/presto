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
package com.facebook.presto.spi;

import com.facebook.presto.common.ErrorCode;
import com.facebook.presto.common.ErrorType;

import static com.facebook.presto.common.ErrorType.EXTERNAL;

/**
 * Error codes for remote function execution.
 * <p>
 * These errors occur when executing functions that run on external services
 * (e.g., remote UDFs, external function services). By default, these errors
 * are NOT catchable by TRY(). However, users can configure the session property
 * try_function_catchable_errors with a comma-separated list of error code names
 * to make TRY() catch these errors and return NULL.
 * <p>
 * Example: SET SESSION try_function_catchable_errors = 'REMOTE_FUNCTION_ERROR,REMOTE_FUNCTION_TIMEOUT'
 * <p>
 * Error code range: 0x0004_0000 - 0x0004_FFFF
 */
public enum RemoteFunctionErrorCode
        implements ErrorCodeSupplier
{
    /**
     * Generic error during remote function execution.
     */
    REMOTE_FUNCTION_ERROR(0, EXTERNAL),

    /**
     * Remote function execution timed out.
     */
    REMOTE_FUNCTION_TIMEOUT(1, EXTERNAL),

    /**
     * Remote function service is unavailable.
     */
    REMOTE_FUNCTION_UNAVAILABLE(2, EXTERNAL),

    /**
     * Invalid response received from remote function.
     */
    REMOTE_FUNCTION_INVALID_RESPONSE(3, EXTERNAL),
    /**/;

    public static final int REMOTE_FUNCTION_ERROR_CODE_MASK = 0x0004_0000;
    public static final int REMOTE_FUNCTION_ERROR_CODE_MAX = 0x0004_FFFF;

    private final ErrorCode errorCode;

    RemoteFunctionErrorCode(int code, ErrorType type)
    {
        // Remote function errors are not catchable by TRY by default.
        // They can be caught by adding their names to the try_function_catchable_errors session property.
        errorCode = new ErrorCode(code + REMOTE_FUNCTION_ERROR_CODE_MASK, name(), type, false, false);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }

    /**
     * Check if the given error code is a remote function error.
     */
    public static boolean isRemoteFunctionError(ErrorCode errorCode)
    {
        int code = errorCode.getCode();
        return code >= REMOTE_FUNCTION_ERROR_CODE_MASK && code <= REMOTE_FUNCTION_ERROR_CODE_MAX;
    }
}
