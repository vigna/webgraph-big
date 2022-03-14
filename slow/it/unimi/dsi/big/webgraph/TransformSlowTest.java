/*
 * Copyright (C) 2011-2022 Sebastiano Vigna
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
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.Test;

import it.unimi.dsi.Util;
import it.unimi.dsi.big.webgraph.Transform.ArcFilter;
import it.unimi.dsi.fastutil.longs.LongBigArrays;
import it.unimi.dsi.util.XoRoShiRo128PlusRandom;

public class TransformSlowTest extends WebGraphTestCase {

	@Test
	public void testTranspose() throws IOException {
		final ImmutableGraph graph = new BVGraphSlowTest.BigGraph(3L << 28, 1 << 2);
		assertTrue(graph.equals(Transform.transposeOffline(Transform.transposeOffline(graph, 1000000000), 1000000000)));
	}

	@Test
	public void testSymmetrize() throws IOException {
		final ImmutableGraph graph = new BVGraphSlowTest.BigGraph(3L << 28, 1 << 2);
		assertTrue(Transform.symmetrizeOffline(graph, 1000000000).equals(Transform.symmetrizeOffline(Transform.symmetrizeOffline(graph, 1000000000), 1000000000)));
		assertTrue(Transform.symmetrizeOffline(graph, 1000000000).equals(Transform.symmetrizeOffline(Transform.transposeOffline(graph, 1000000000), 1000000000)));
	}

	@Test
	public void testMap() throws IOException {
		final ImmutableGraph graph = new BVGraphSlowTest.BigGraph((2L << 20) + 1, 1 << 10);
		final long[][] perm = Util.identity(graph.numNodes());
		LongBigArrays.shuffle(perm, new XoRoShiRo128PlusRandom(0));
		final long[][] inv = Util.invertPermutation(perm);
		assertTrue(graph.equals(Transform.mapOffline(Transform.mapOffline(graph, perm, 1000000000), inv, 1000000000)));
	}

	@Test
	public void testFilter() {
		final ImmutableGraph graph = new BVGraphSlowTest.BigGraph((2L << 20) + 1, 1 << 10);
		// Just testings that the basic implementation is OK.
		assertEquals(graph, Transform.filterArcs(graph, (ArcFilter) (i, t) -> true));
	}
}
