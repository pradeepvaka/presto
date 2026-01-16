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
import org.testng.annotations.Test;

import static com.facebook.presto.spi.RemoteFunctionErrorCode.REMOTE_FUNCTION_ERROR;
import static com.facebook.presto.spi.RemoteFunctionErrorCode.REMOTE_FUNCTION_ERROR_CODE_MASK;
import static com.facebook.presto.spi.RemoteFunctionErrorCode.REMOTE_FUNCTION_ERROR_CODE_MAX;
import static com.facebook.presto.spi.RemoteFunctionErrorCode.REMOTE_FUNCTION_INVALID_RESPONSE;
import static com.facebook.presto.spi.RemoteFunctionErrorCode.REMOTE_FUNCTION_TIMEOUT;
import static com.facebook.presto.spi.RemoteFunctionErrorCode.REMOTE_FUNCTION_UNAVAILABLE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestRemoteFunctionErrorCode
{
    @Test
    public void testErrorCodeValues()
    {
        // Verify error codes are in the expected range
        assertEquals(REMOTE_FUNCTION_ERROR.toErrorCode().getCode(), REMOTE_FUNCTION_ERROR_CODE_MASK);
        assertEquals(REMOTE_FUNCTION_TIMEOUT.toErrorCode().getCode(), REMOTE_FUNCTION_ERROR_CODE_MASK + 1);
        assertEquals(REMOTE_FUNCTION_UNAVAILABLE.toErrorCode().getCode(), REMOTE_FUNCTION_ERROR_CODE_MASK + 2);
        assertEquals(REMOTE_FUNCTION_INVALID_RESPONSE.toErrorCode().getCode(), REMOTE_FUNCTION_ERROR_CODE_MASK + 3);
    }

    @Test
    public void testErrorCodeType()
    {
        // All remote function errors should be EXTERNAL type
        for (RemoteFunctionErrorCode errorCode : RemoteFunctionErrorCode.values()) {
            assertEquals(
                    errorCode.toErrorCode().getType(),
                    ErrorType.EXTERNAL,
                    "Error " + errorCode.name() + " should be EXTERNAL type");
        }
    }

    @Test
    public void testErrorCodeNotCatchableByTryByDefault()
    {
        // Remote function errors should NOT be catchable by TRY by default
        for (RemoteFunctionErrorCode errorCode : RemoteFunctionErrorCode.values()) {
            assertFalse(
                    errorCode.toErrorCode().isCatchableByTry(),
                    "Error " + errorCode.name() + " should not be catchable by TRY by default");
        }
    }

    @Test
    public void testAllErrorCodesHaveUniqueNumbers()
    {
        RemoteFunctionErrorCode[] values = RemoteFunctionErrorCode.values();
        for (int i = 0; i < values.length; i++) {
            for (int j = i + 1; j < values.length; j++) {
                assertTrue(
                        values[i].toErrorCode().getCode() != values[j].toErrorCode().getCode(),
                        "Duplicate error code: " + values[i] + " and " + values[j]);
            }
        }
    }

    @Test
    public void testIsRemoteFunctionError()
    {
        // All defined error codes should be detected as remote function errors
        for (RemoteFunctionErrorCode errorCode : RemoteFunctionErrorCode.values()) {
            assertTrue(
                    RemoteFunctionErrorCode.isRemoteFunctionError(errorCode.toErrorCode()),
                    "Error " + errorCode.name() + " should be detected as remote function error");
        }

        // Boundary conditions
        ErrorCode atRangeStart = new ErrorCode(REMOTE_FUNCTION_ERROR_CODE_MASK, "TEST", ErrorType.EXTERNAL);
        assertTrue(RemoteFunctionErrorCode.isRemoteFunctionError(atRangeStart));

        ErrorCode atRangeEnd = new ErrorCode(REMOTE_FUNCTION_ERROR_CODE_MAX, "TEST", ErrorType.EXTERNAL);
        assertTrue(RemoteFunctionErrorCode.isRemoteFunctionError(atRangeEnd));

        // Outside range
        ErrorCode belowRange = new ErrorCode(REMOTE_FUNCTION_ERROR_CODE_MASK - 1, "TEST", ErrorType.USER_ERROR);
        assertFalse(RemoteFunctionErrorCode.isRemoteFunctionError(belowRange));

        ErrorCode aboveRange = new ErrorCode(REMOTE_FUNCTION_ERROR_CODE_MAX + 1, "TEST", ErrorType.USER_ERROR);
        assertFalse(RemoteFunctionErrorCode.isRemoteFunctionError(aboveRange));
    }

    @Test
    public void testErrorCodeRange()
    {
        // Verify the range constants are correct
        assertEquals(REMOTE_FUNCTION_ERROR_CODE_MASK, 0x0004_0000);
        assertEquals(REMOTE_FUNCTION_ERROR_CODE_MAX, 0x0004_FFFF);

        // All error codes should be within the range
        for (RemoteFunctionErrorCode errorCode : RemoteFunctionErrorCode.values()) {
            int code = errorCode.toErrorCode().getCode();
            assertTrue(
                    code >= REMOTE_FUNCTION_ERROR_CODE_MASK && code <= REMOTE_FUNCTION_ERROR_CODE_MAX,
                    "Error code " + errorCode.name() + " should be in remote function range");
        }
    }
}
