/*
 * Copyright (C) 2007-2022 Paolo Boldi and Sebastiano Vigna
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

import java.util.Arrays;

/**
 * An abstract (single-attribute) list-of-longs label.
 *
 * <p>
 * This class provides basic methods for a label holding a list of longs. Concrete implementations
 * may impose further requirements on the long.
 *
 * <p>
 * Implementing subclasses must provide constructors, {@link Label#copy()},
 * {@link Label#fromBitStream(it.unimi.dsi.io.InputBitStream, int)},
 * {@link Label#toBitStream(it.unimi.dsi.io.OutputBitStream, int)} and possibly override
 * {@link #toString()}.
 */

public abstract class AbstractLongListLabel extends AbstractLabel implements Label {
    /** The key of the attribute represented by this label. */
    protected final String key;
    /** The values of the attribute represented by this label. */
    public long[] value;

    /**
     * Creates an long label with given key and value.
     *
     * @param key the (only) key of this label.
     * @param value the value of this label.
     */
    public AbstractLongListLabel(String key, long[] value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public String wellKnownAttributeKey() {
        return key;
    }

    @Override
    public String[] attributeKeys() {
        return new String[]{key};
    }

    public Class<?>[] attributeTypes() { return new Class<?>[] { long[].class }; }

    @Override
    public Object get(String key) {
        if (this.key.equals(key))
            return value;
        throw new IllegalArgumentException();
    }

    @Override
    public Object get() {
        return value;
    }

    @Override
    public String toString() {
        return key + ":" + Arrays.toString(value);
    }

    @Override
    public boolean equals(Object x) {
        if (x instanceof AbstractLongListLabel)
            return Arrays.equals(value, ((AbstractLongListLabel) x).value);
        else
            return false;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(value);
    }
}
