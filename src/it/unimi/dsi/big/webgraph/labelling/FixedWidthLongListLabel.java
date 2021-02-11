/*
 * Copyright (C) 2007-2021 Paolo Boldi and Sebastiano Vigna
 *
 * This program and the accompanying materials are made available under the
 * terms of the GNU Lesser General Public License v2.1 or later,
 * which is available at
 * http://www.gnu.org/licenses/old-licenses/lgpl-2.1-standalone.html,
 * or the Apache Software License 2.0, which is available at
 * https://www.apache.org/licenses/LICENSE-2.0.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later OR Apache-2.0
 */

package it.unimi.dsi.big.webgraph.labelling;

import java.io.IOException;
import java.util.Arrays;

import it.unimi.dsi.fastutil.longs.LongArrays;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;

/**
 * A list of longs represented in fixed width. The provided width must
 * be smaller than 64. Each list is prefixed by its length written in
 * {@linkplain OutputBitStream#writeGamma(int) &gamma; coding}.
 */

public class FixedWidthLongListLabel extends AbstractLongListLabel {
    /** The bit width used to represent the value of this label. */
    private final int width;

    /**
     * Creates a new fixed-width long label.
     *
     * @param key the (only) key of this label.
     * @param width the label width (in bits).
     * @param value the value of this label.
     */
    public FixedWidthLongListLabel(String key, int width, long[] value) {
        super(key, value);
        if (width < 0 || width > 63) throw new IllegalArgumentException("Width out of range: " + width);
        for (int i = value.length; i-- != 0;)
            if (value[i] < 0 || value[i] >= 1L << width)
                throw new IllegalArgumentException("Value out of range: " + Long.toString(value[i]));
        this.width = width;
    }

    /**
     * Creates a new fixed-width label with an empty list.
     *
     * @param key the (only) key of this label.
     * @param width the label width (in bits).
     */
    public FixedWidthLongListLabel(String key, int width) {
        this(key, width, LongArrays.EMPTY_ARRAY);
    }

    /**
     * Creates a new fixed-width long label using the given key and width with an empty list.
     *
     * @param arg two strings containing the key and the width of this label.
     */
    public FixedWidthLongListLabel(String... arg) {
        this(arg[0], Integer.parseInt(arg[1]));
    }

    @Override
    public Label copy() {
        return new FixedWidthLongListLabel(key, width, value.clone());
    }

    @Override
    public int fromBitStream(InputBitStream inputBitStream, final long sourceUnused) throws IOException {
        long readBits = inputBitStream.readBits();
        value = new long[inputBitStream.readGamma()];
        for (int i = 0; i < value.length; i++)
            value[i] = inputBitStream.readLong(width);
        return (int) (inputBitStream.readBits() - readBits);
    }

    @Override
    public int toBitStream(OutputBitStream outputBitStream, final long sourceUnused) throws IOException {
        int bits = outputBitStream.writeGamma(value.length);
        for (int i = 0; i < value.length; i++)
            bits += outputBitStream.writeLong(value[i], width);
        return bits;
    }

    /**
     * Returns -1 (the fixed width refers to a single long, not to the entire list).
     * 
     * @return -1;
     */
    @Override
    public int fixedWidth() {
        return -1;
    }

    @Override
    public String toString() {
        return key + ":" + Arrays.toString(value) + " (width:" + width + ")";
    }

    @Override
    public String toSpec() {
        return this.getClass().getName() + "(" + key + "," + width + ")";
    }
}
