/*
 * Copyright (C) 2004 The Guava Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.common.io;

import java.io.OutputStream;

/**
 * Implementation of {@link java.io.OutputStream} that simply discards written bytes.
 *
 * Note this class is in our codebase because HBase uses it internally (even though they relocate all of Guava!).
 * The class was removed from Guava 15.0, and we want to be able to move to that version of Guava.  However, since
 * the class was removed, it results in the embedded version of HBase being unable to startup due to the class not
 * being found.  So, we put it into our own codebase which then makes it available on the classpath for the embedded
 * HBase to be able to startup successfully.
 *
 * @author Spencer Kimball
 * @since 1.0
 */
public final class NullOutputStream extends OutputStream {
    /** Discards the specified byte. */
    @Override public void write(int b) {
    }

    /** Discards the specified byte array. */
    @Override public void write(byte[] b, int off, int len) {
    }
}
