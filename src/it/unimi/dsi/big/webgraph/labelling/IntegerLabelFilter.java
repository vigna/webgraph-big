/*
 * Copyright (C) 2008-2020 Sebastiano Vigna
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

import it.unimi.dsi.big.webgraph.Transform.LabelledArcFilter;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;

public class IntegerLabelFilter implements LabelledArcFilter {
	/** The values of the label that will be preserved. */
	private final IntOpenHashSet values;
	private final String key;

	public IntegerLabelFilter(final String key, final int... value) {
		this.key = key;
		values = new IntOpenHashSet(value);
	}

	public IntegerLabelFilter(final String... keyAndvalues) {
		if (keyAndvalues.length == 0) throw new IllegalArgumentException("You must specificy a key name");
		this.key = keyAndvalues[0].length() == 0 ? null : keyAndvalues[0];
		values = new IntOpenHashSet(keyAndvalues.length);
		for(int i = 1; i < keyAndvalues.length; i++) values.add(Integer.parseInt(keyAndvalues[i]));
	}

	@Override
	public boolean accept(final long i, final long j, final Label label) {
		return values.contains(key == null ? label.getInt() : label.getInt(key));
	}
}
