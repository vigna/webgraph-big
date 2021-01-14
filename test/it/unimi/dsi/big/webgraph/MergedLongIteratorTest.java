/*
 * Copyright (C) 2003-2020 Paolo Boldi
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

package it.unimi.dsi.big.webgraph;

import static org.junit.Assert.assertEquals;

import java.util.Random;

import org.junit.Test;

import it.unimi.dsi.fastutil.longs.LongAVLTreeSet;
import it.unimi.dsi.fastutil.longs.LongIterator;

public class MergedLongIteratorTest {

	public void testMerge(final int n0, final int n1) {
		final long x0[] = new long[n0];
		final long x1[] = new long[n1];
		int i;
		long p = 0;
		final Random random = new Random();

		// Generate
		for (i = 0; i < n0; i++) p = x0[i] = p + random.nextInt(10);
		p = 0;
		for (i = 0; i < n1; i++) p = x1[i] = p + random.nextInt(10);

		final LongAVLTreeSet s0 = new LongAVLTreeSet(x0);
		final LongAVLTreeSet s1 = new LongAVLTreeSet(x1);
		final LongAVLTreeSet res = new LongAVLTreeSet(s0);
		res.addAll(s1);

		final MergedLongIterator m = new MergedLongIterator(LazyLongIterators.lazy(s0.iterator()), LazyLongIterators.lazy(s1.iterator()));
		final LongIterator it = res.iterator();

		long x;
		while ((x = m.nextLong()) != -1) assertEquals(it.nextLong(), x);
		assertEquals(Boolean.valueOf(it.hasNext()), Boolean.valueOf(m.nextLong() != -1));
	}

		@Test
	public void testMerge() {
		for(int i = 0; i < 10; i++) {
			testMerge(i, i);
			testMerge(i, i + 1);
			testMerge(i, i * 2);
		}
	}
}
