/**
 * Copyright (C) 2006-2009 Dustin Sallings
 * Copyright (C) 2009-2011 Couchbase, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALING
 * IN THE SOFTWARE.
 */

package net.spy.memcached.protocol.binary;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import com.sun.org.apache.xerces.internal.impl.dv.util.Base64;

import net.spy.memcached.ops.GetOperation;

/**
 * Implementation of the get operation.
 */
public class GetObjOperationImpl extends SingleKeyOperationImpl implements GetOperation {

//  static final byte GET_CMD = 0x0D;

  static final byte GET_CMD = 0x13;

  /**
   * Length of the extra header stuff for a GET response.
   */
  static final int EXTRA_HDR_LEN = 4;

  public GetObjOperationImpl(String k, GetOperation.Callback cb) {
    super(GET_CMD, generateOpaque(), k, cb);
  }

  @Override
  public void initialize() {
	short flag = 1;
    prepareBuffer(key, 0, EMPTY_BYTES, flag, flag);
  }

  protected void prepareBuffer(final String key, final long cas,
		    final byte[] val, final Object... extraHeaders) {
		    int extraLen = 0;
		    int extraHeadersLength = extraHeaders.length;

		    if (extraHeadersLength > 0) {
		      extraLen = calculateExtraLength(extraHeaders);
		    }

		    final byte[] keyBytes =Base64.decode(key);
		    int bufSize = MIN_RECV_PACKET + keyBytes.length + val.length;

		    ByteBuffer bb = ByteBuffer.allocate(bufSize + extraLen);
		    assert bb.order() == ByteOrder.BIG_ENDIAN;
		    bb.put(REQ_MAGIC);
		    bb.put(cmd);
		    bb.putShort((short) keyBytes.length);
		    bb.put((byte) extraLen);
		    bb.put((byte) 0);
		    bb.putShort(vbucket);
		    bb.putInt(keyBytes.length + val.length + extraLen);
		    bb.putInt(opaque);
		    bb.putLong(cas);

		    if (extraHeadersLength > 0) {
		      addExtraHeaders(bb, extraHeaders);
		    }

		    bb.put(keyBytes);
		    bb.put(val);

		    bb.flip();
		    setBuffer(bb);
		  }
  
  @Override
  protected void decodePayload(byte[] pl) {
    final int flags = decodeInt(pl, 0);
    final byte[] data = new byte[pl.length - EXTRA_HDR_LEN];
    System.arraycopy(pl, EXTRA_HDR_LEN, data, 0, pl.length - EXTRA_HDR_LEN);
    GetOperation.Callback gcb = (GetOperation.Callback) getCallback();
    gcb.gotData(key, flags, data);
    getCallback().receivedStatus(STATUS_OK);
  }
}
