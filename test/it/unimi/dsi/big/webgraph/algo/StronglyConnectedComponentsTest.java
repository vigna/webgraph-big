/*
 * Copyright (C) 2007-2020 Sebastiano Vigna
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

package it.unimi.dsi.big.webgraph.algo;

import static it.unimi.dsi.fastutil.BigArrays.get;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

import it.unimi.dsi.big.webgraph.ImmutableGraph;
import it.unimi.dsi.big.webgraph.WebGraphTestCase;
import it.unimi.dsi.bits.LongArrayBitVector;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.ArrayListMutableGraph;
import it.unimi.dsi.webgraph.examples.ErdosRenyiGraph;

public class StronglyConnectedComponentsTest extends WebGraphTestCase {

	public static void sameComponents(final long l, final StronglyConnectedComponentsTarjan componentsRecursive, final StronglyConnectedComponents componentsIterative) {
		final LongOpenHashSet[] recursiveComponentsSet = new LongOpenHashSet[(int)componentsRecursive.numberOfComponents];
		final LongOpenHashSet[] iterativeComponentsSet = new LongOpenHashSet[(int)componentsIterative.numberOfComponents];

		for(int i = recursiveComponentsSet.length; i-- != 0;) {
			recursiveComponentsSet[i] = new LongOpenHashSet();
			iterativeComponentsSet[i] = new LongOpenHashSet();
		}

		for(long i = l; i-- != 0;) {
			recursiveComponentsSet[(int)get(componentsRecursive.component, i)].add(i);
			iterativeComponentsSet[(int)get(componentsIterative.component, i)].add(i);
		}

		assertEquals(new ObjectOpenHashSet<>(recursiveComponentsSet), new ObjectOpenHashSet<>(iterativeComponentsSet));
	}

	@Test
	public void testBuckets() {
		final ImmutableGraph g = ImmutableGraph.wrap(new ArrayListMutableGraph(9,
				new int[][] { { 0, 0 }, { 1, 0 }, { 1, 2 },
				{ 2, 1 }, { 2, 3 }, { 2, 4 }, { 2, 5 },
				{ 3, 4 }, { 4, 3 },
				{ 5, 5 }, { 5, 6 }, { 5, 7 }, { 5, 8 },
				{ 6, 7 },
				{ 8, 7 } }
		).immutableView());

		final StronglyConnectedComponents components = StronglyConnectedComponents.compute(g, true, null);

		final LongArrayBitVector buckets = LongArrayBitVector.ofLength(g.numNodes());
		buckets.set(0, true);
		buckets.set(3, true);
		buckets.set(4, true);
		assertEquals(buckets, components.buckets);
		assertEquals(3, buckets.count());

		final long[][] size = components.computeSizes();
		components.sortBySize(size);

		assertEquals(2, get(size, 0));
		assertEquals(2, get(size, 1));
		assertEquals(1, get(size, 2));
		assertEquals(1, get(size, 3));
		assertEquals(1, get(size, 4));
		assertEquals(1, get(size, 5));
		assertEquals(1, get(size, 6));

		StronglyConnectedComponents.compute(g, false, null); // To increase coverage
	}

	@Test
	public void testBuckets2() {
		final ImmutableGraph g = ImmutableGraph.wrap(new ArrayListMutableGraph(4,
				new int[][] { { 0, 1 }, { 1, 2 }, { 2, 0 }, { 1, 3 }, { 3, 3 } }
		).immutableView());

		final StronglyConnectedComponents components = StronglyConnectedComponents.compute(g, true, null);

		final LongArrayBitVector buckets = LongArrayBitVector.ofLength(g.numNodes());
		buckets.set(3);
		assertEquals(buckets, components.buckets);
		assertEquals(1, buckets.count());
	}


	@Test
	public void testCompleteGraph() {
		final StronglyConnectedComponents components = StronglyConnectedComponents.compute(ImmutableGraph.wrap(ArrayListMutableGraph.newCompleteGraph(5, false).immutableView()), true, null);
		assertEquals(5, components.buckets.count());
		for (int i = 5; i-- != 0;) assertEquals(0, get(components.component, i));
		assertEquals(5, components.computeSizes()[0][0]);
	}

	@Test
	public void testNoBuckets() {
		StronglyConnectedComponentsTarjan.compute(ImmutableGraph.wrap(ArrayListMutableGraph.newCompleteGraph(5, false).immutableView()), false, null);
	}

	@Test
	public void testWithProgressLogger() {
		StronglyConnectedComponentsTarjan.compute(ImmutableGraph.wrap(ArrayListMutableGraph.newCompleteGraph(5, false).immutableView()), true, new ProgressLogger());
	}

	@Test
	public void testTree() {
		final StronglyConnectedComponents components = StronglyConnectedComponents.compute(ImmutableGraph.wrap(ArrayListMutableGraph.newCompleteBinaryIntree(3).immutableView()), true, null);
		assertEquals(0, components.buckets.count());
		assertEquals(15, components.numberOfComponents);
	}

	@Test
	public void testErdosRrenyi() {
		for(final int size: new int[] { 10, 100, 1000 }) {
			for(int attempt = 0; attempt < 5; attempt++) {
				final ImmutableGraph view = ImmutableGraph.wrap(new ArrayListMutableGraph(new ErdosRenyiGraph(size, .05, attempt + 1, false)).immutableView());
				final StronglyConnectedComponentsTarjan componentsRecursive = StronglyConnectedComponentsTarjan.compute(view, true, null);
				final StronglyConnectedComponents componentsIterative = StronglyConnectedComponents.compute(view, true, null);
				assertEquals(componentsRecursive.numberOfComponents, componentsIterative.numberOfComponents);
				sameComponents(size, componentsRecursive, componentsIterative);
			}
		}
	}
}
