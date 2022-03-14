/*
 * Copyright (C) 2007-2022 Sebastiano Vigna
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

import static it.unimi.dsi.fastutil.BigArrays.length;
import static it.unimi.dsi.fastutil.BigArrays.set;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import org.junit.Test;

import it.unimi.dsi.Util;
import it.unimi.dsi.big.webgraph.Transform.ArcFilter;
import it.unimi.dsi.big.webgraph.Transform.LabelledArcFilter;
import it.unimi.dsi.big.webgraph.examples.IntegerTriplesArcLabelledImmutableGraph;
import it.unimi.dsi.big.webgraph.labelling.ArcLabelledImmutableGraph;
import it.unimi.dsi.big.webgraph.labelling.ArcLabelledNodeIterator;
import it.unimi.dsi.big.webgraph.labelling.ArcLabelledNodeIterator.LabelledArcIterator;
import it.unimi.dsi.big.webgraph.labelling.BitStreamArcLabelledGraphTest;
import it.unimi.dsi.big.webgraph.labelling.GammaCodedIntLabel;
import it.unimi.dsi.big.webgraph.labelling.Label;
import it.unimi.dsi.big.webgraph.labelling.LabelSemiring;
import it.unimi.dsi.fastutil.BigArrays;
import it.unimi.dsi.fastutil.longs.LongBigArrays;
import it.unimi.dsi.util.XoRoShiRo128PlusRandom;
import it.unimi.dsi.webgraph.ArrayListMutableGraph;
import it.unimi.dsi.webgraph.ImmutableSequentialGraph;
import it.unimi.dsi.webgraph.examples.ErdosRenyiGraph;

public class TransformTest extends WebGraphTestCase {

	public static File storeTempGraph(final ImmutableGraph g) throws IOException, IllegalArgumentException, SecurityException {
		final File basename = File.createTempFile(TransformTest.class.getSimpleName(), "-test");
		BVGraph.store(g, basename.toString());
		return basename;
	}

	@Test
	public void testMapExpand() throws IOException {
		ImmutableGraph g;
		ImmutableGraph g2;

		g = ImmutableGraph.wrap(ArrayListMutableGraph.newCompleteGraph(4, false).immutableView());
		g2 = Transform.mapOffline(g, BigArrays.wrap(new long[] { 0, 2, 4, 6 }), 10);
		assertGraph(g2);
		assertEquals(ImmutableGraph.wrap(new ArrayListMutableGraph(7, (i, j) -> i % 2 == 0 && j % 2 == 0 && i != j).immutableView()), g2);

		g = ImmutableGraph.wrap(ArrayListMutableGraph.newDirectedCycle(3).immutableView());
		g2 = Transform.mapOffline(g, BigArrays.wrap(new long[] { 0, 3, 3 }), 10);
		assertGraph(g2);
		assertEquals(ImmutableGraph.wrap(new ArrayListMutableGraph(4, new int[][] { { 0, 3 }, { 3, 0 }, { 3, 3 } }).immutableView()), g2);

		g = ImmutableGraph.wrap(ArrayListMutableGraph.newDirectedCycle(3).immutableView());
		g2 = Transform.mapOffline(g, BigArrays.wrap(new long[] { 4, 4, 4 }), 10);
		assertGraph(g2);
		assertEquals(ImmutableGraph.wrap(new ArrayListMutableGraph(5, new int[][] { { 4, 4 } }).immutableView()), g2);

		g = ImmutableGraph.wrap(ArrayListMutableGraph.newDirectedCycle(3).immutableView());
		g2 = Transform.mapOffline(g, BigArrays.wrap(new long[] { 6, 5, 4 }), 10);
		assertGraph(g2);
		assertEquals(ImmutableGraph.wrap(new ArrayListMutableGraph(7, new int[][] { { 6, 5 }, { 5, 4 }, { 4, 6 } }).immutableView()), g2);

	}

	@Test
	public void testMapPermutation() throws IOException {
		ImmutableGraph g;
		ImmutableGraph g2;

		g = ImmutableGraph.wrap(ArrayListMutableGraph.newDirectedCycle(3).immutableView());
		g2 = Transform.mapOffline(g, BigArrays.wrap(new long[] { 2, 1, 0 }), 10);
		assertGraph(g2);
		assertEquals(ImmutableGraph.wrap(new ArrayListMutableGraph(3, new int[][] { { 0, 2 }, { 2, 1 }, { 1, 0 } }).immutableView()), g2);
	}

	@Test
	public void testInjective() throws IOException {
		ImmutableGraph g;
		ImmutableGraph g2;

		g = ImmutableGraph.wrap(new ArrayListMutableGraph(3, new int[][] { { 0, 1 }, { 1, 2 }, { 0, 2 } }).immutableView());
		g2 = Transform.mapOffline(g, BigArrays.wrap(new long[] { 2, -1, 0 }), 10);
		assertGraph(g2);
		assertEquals(ImmutableGraph.wrap(new ArrayListMutableGraph(3, new int[][] { { 2, 0 } }).immutableView()), g2);
	}

	@Test
	public void testMapCollapse() throws IOException {
		ImmutableGraph g;
		ImmutableGraph g2;

		g = ImmutableGraph.wrap(ArrayListMutableGraph.newDirectedCycle(3).immutableView());
		g2 = Transform.mapOffline(g, BigArrays.wrap(new long[] { 0, 0, 0 }), 10);
		assertGraph(g2);
		assertEquals(1, g2.numNodes());
	}

	@Test
	public void testMapClear() throws IOException {
		ImmutableGraph g;
		ImmutableGraph g2;

		g = ImmutableGraph.wrap(ArrayListMutableGraph.newDirectedCycle(3).immutableView());
		g2 = Transform.mapOffline(g, BigArrays.wrap(new long[] { -1, -1, -1 }), 10);
		assertGraph(g2);
		assertEquals(0, g2.numNodes());
	}

	@Test
	public void testMapKeepMiddle() throws IOException {
		ImmutableGraph g;
		ImmutableGraph g2;

		g = ImmutableGraph.wrap(ArrayListMutableGraph.newDirectedCycle(3).immutableView());
		g2 = Transform.mapOffline(g, BigArrays.wrap(new long[] { -1, 0, -1 }), 10);
		assertGraph(g2);
		assertEquals(ImmutableGraph.wrap(ArrayListMutableGraph.newCompleteGraph(1, false).immutableView()), g2);

		g = ImmutableGraph.wrap(ArrayListMutableGraph.newDirectedCycle(3).immutableView());
		g2 = Transform.mapOffline(g, BigArrays.wrap(new long[] { -1, 2, -1 }), 10);
		assertGraph(g2);
		assertEquals(ImmutableGraph.wrap(new ArrayListMutableGraph(3, new int[][] {}).immutableView()), g2);
	}

	@Test
	public void testLex() {
		ImmutableGraph g = ImmutableGraph.wrap(new ArrayListMutableGraph(3, new int[][] { { 0, 2 }, { 1, 1 }, { 1, 2 }, { 2, 0 }, { 2, 1 }, { 2, 2 } }).immutableView());
		long[][] p = Transform.lexicographicalPermutation(g);
		assertArrayEquals(BigArrays.wrap(new long[] { 0, 1, 2 }), p);

		g = ImmutableGraph.wrap(new ArrayListMutableGraph(3, new int[][] { { 0, 0 }, { 0, 1 }, { 0, 2 }, { 1, 1 }, { 1, 2 }, { 2, 2 } }).immutableView());
		p = Transform.lexicographicalPermutation(g);
		assertArrayEquals(BigArrays.wrap(new long[] { 2, 1, 0 }), p);
	}


	@Test
	public void testFilters() throws IllegalArgumentException, SecurityException {
		final ImmutableGraph graph = ImmutableGraph.wrap(new ArrayListMutableGraph(6,
				new int[][] {
						{ 0, 1 },
						{ 0, 2 },
						{ 1, 1 },
						{ 1, 3 },
						{ 2, 1 },
						{ 4, 5 },
				}
		).immutableView());

		final ImmutableGraph filtered = Transform.filterArcs(graph, (ArcFilter)(i, j) -> i < j, null);

		assertGraph(filtered);

		final NodeIterator nodeIterator = filtered.nodeIterator();
		LazyLongIterator iterator;
		assertTrue(nodeIterator.hasNext());
		assertEquals(0, nodeIterator.nextLong());
		iterator = nodeIterator.successors();
		assertEquals(1, iterator.nextLong());
		assertEquals(2, iterator.nextLong());
		assertEquals(-1, iterator.nextLong());
		assertTrue(nodeIterator.hasNext());
		assertEquals(1, nodeIterator.nextLong());
		iterator = nodeIterator.successors();
		assertEquals(3, iterator.nextLong());
		assertEquals(-1, iterator.nextLong());
		assertTrue(nodeIterator.hasNext());
		assertEquals(2, nodeIterator.nextLong());
		iterator = nodeIterator.successors();
		assertEquals(-1, iterator.nextLong());
		assertTrue(nodeIterator.hasNext());
		assertEquals(3, nodeIterator.nextLong());
		iterator = nodeIterator.successors();
		assertEquals(-1, iterator.nextLong());
		assertEquals(4, nodeIterator.nextLong());
		iterator = nodeIterator.successors();
		assertEquals(5, iterator.nextLong());
		assertEquals(-1, iterator.nextLong());
		assertTrue(nodeIterator.hasNext());
		assertEquals(5, nodeIterator.nextLong());
		iterator = nodeIterator.successors();
		assertEquals(-1, iterator.nextLong());
		assertFalse(nodeIterator.hasNext());
	}


	@Test
	public void testLabelledFilters() throws IllegalArgumentException, SecurityException, IOException {
		final IntegerTriplesArcLabelledImmutableGraph graph = new IntegerTriplesArcLabelledImmutableGraph(
				new int[][] {
						{ 0, 1, 2 },
						{ 0, 2, 3 },
						{ 1, 1, 4 },
						{ 1, 3, 5 },
						{ 2, 1, 6 },
						{ 4, 5, 7 },
				}
		);

		ArcLabelledImmutableGraph filtered = Transform.filterArcs(graph, (LabelledArcFilter)(i, j, label) -> i < j, null);

		final ArcLabelledNodeIterator nodeIterator = filtered.nodeIterator();
		LabelledArcIterator iterator;
		assertTrue(nodeIterator.hasNext());
		assertEquals(0, nodeIterator.nextLong());
		iterator = nodeIterator.successors();
		assertEquals(1, iterator.nextLong());
		assertEquals(2, iterator.label().getInt());
		assertEquals(2, iterator.nextLong());
		assertEquals(3, iterator.label().getInt());
		assertEquals(-1, iterator.nextLong());
		assertTrue(nodeIterator.hasNext());
		assertEquals(1, nodeIterator.nextLong());
		iterator = nodeIterator.successors();
		assertEquals(3, iterator.nextLong());
		assertEquals(5, iterator.label().getInt());
		assertEquals(-1, iterator.nextLong());
		assertTrue(nodeIterator.hasNext());
		assertEquals(2, nodeIterator.nextLong());
		iterator = nodeIterator.successors();
		assertEquals(-1, iterator.nextLong());
		assertTrue(nodeIterator.hasNext());
		assertEquals(3, nodeIterator.nextLong());
		iterator = nodeIterator.successors();
		assertEquals(-1, iterator.nextLong());
		assertEquals(4, nodeIterator.nextLong());
		iterator = nodeIterator.successors();
		assertEquals(5, iterator.nextLong());
		assertEquals(7, iterator.label().getInt());
		assertEquals(-1, iterator.nextLong());
		assertTrue(nodeIterator.hasNext());
		assertEquals(5, nodeIterator.nextLong());
		iterator = nodeIterator.successors();
		assertEquals(-1, iterator.nextLong());
		assertFalse(nodeIterator.hasNext());

		final File file = BitStreamArcLabelledGraphTest.storeTempGraph(graph);
		final ArcLabelledImmutableGraph graph2 = ArcLabelledImmutableGraph.load(file.toString());

		filtered = Transform.filterArcs(graph2, (LabelledArcFilter)(i, j, label) -> i < j, null);

		iterator = filtered.successors(0);
		assertEquals(1, iterator.nextLong());
		assertEquals(2, iterator.label().getInt());
		assertEquals(2, iterator.nextLong());
		assertEquals(3, iterator.label().getInt());
		assertEquals(-1, iterator.nextLong());
		iterator = filtered.successors(1);
		assertEquals(3, iterator.nextLong());
		assertEquals(5, iterator.label().getInt());
		assertEquals(-1, iterator.nextLong());
		iterator = filtered.successors(2);
		assertEquals(-1, iterator.nextLong());
		iterator = filtered.successors(3);
		assertEquals(-1, iterator.nextLong());
		iterator = filtered.successors(4);
		assertEquals(5, iterator.nextLong());
		assertEquals(7, iterator.label().getInt());
		assertEquals(-1, iterator.nextLong());
		iterator = filtered.successors(5);
		assertEquals(-1, iterator.nextLong());

	}

	@Test
	public void testCompose() {
		final ImmutableGraph g0 = ImmutableGraph.wrap(new ArrayListMutableGraph(3, new int[][]  { { 0, 1 }, { 0, 2 } }).immutableView());
		final ImmutableGraph g1 = ImmutableGraph.wrap(new ArrayListMutableGraph(3, new int[][]  { { 1, 0 }, { 2, 1 } }).immutableView());

		final ImmutableGraph c = Transform.compose(g0, g1);

		final NodeIterator n = c.nodeIterator();
		assertTrue(n.hasNext());
		assertEquals(0, n.nextLong());
		LazyLongIterator i = n.successors();
		assertEquals(0, i.nextLong());
		assertEquals(1, i.nextLong());
		assertEquals(-1, i.nextLong());
		assertEquals(1, n.nextLong());
		i = n.successors();
		assertEquals(-1, i.nextLong());
		assertTrue(n.hasNext());
		assertEquals(2, n.nextLong());
		i = n.successors();
		assertEquals(-1, i.nextLong());
		assertFalse(n.hasNext());

		assertEquals(c, c.copy());
		assertEquals(c.copy(), c);
	}

	@Test
	public void testLabelledCompose() throws IllegalArgumentException, SecurityException, IOException {
		final File file = BitStreamArcLabelledGraphTest.storeTempGraph(new IntegerTriplesArcLabelledImmutableGraph(
				new int[][] {
						{ 0, 1, 2 },
						{ 0, 2, 10 },
						{ 0, 3, 1 },
						{ 1, 2, 4 },
						{ 3, 2, 1 },
				}
		));
		final ArcLabelledImmutableGraph graph = ArcLabelledImmutableGraph.load(file.toString());

		final ArcLabelledImmutableGraph composed = Transform.compose(graph, graph, new LabelSemiring() {
			private final GammaCodedIntLabel one = new GammaCodedIntLabel("FOO");
			private final GammaCodedIntLabel zero = new GammaCodedIntLabel("FOO");
			{
				one.value = 0;
				zero.value = Integer.MAX_VALUE;
			}

			@Override
			public Label add(final Label first, final Label second) {
				final GammaCodedIntLabel result = new GammaCodedIntLabel("FOO");
				result.value = Math.min(first.getInt(), second.getInt());
				return result;
			}

			@Override
			public Label multiply(final Label first, final Label second) {
				final GammaCodedIntLabel result = new GammaCodedIntLabel("FOO");
				result.value = first.getInt() + second.getInt();
				return result;
			}

			@Override
			public Label one() {
				return one;
			}

			@Override
			public Label zero() {
				return zero;
			}
		});

		final ArcLabelledNodeIterator n = composed.nodeIterator();
		assertTrue(n.hasNext());
		assertEquals(0, n.nextLong());
		LabelledArcIterator i = n.successors();
		assertEquals(2, i.nextLong());
		assertEquals(2, i.label().getInt());
		assertEquals(-1, i.nextLong());
		assertEquals(1, n.nextLong());
		i = n.successors();
		assertEquals(-1, i.nextLong());
		assertTrue(n.hasNext());
		assertEquals(2, n.nextLong());
		i = n.successors();
		assertEquals(-1, i.nextLong());
		assertTrue(n.hasNext());
		assertEquals(3, n.nextLong());
		i = n.successors();
		assertEquals(-1, i.nextLong());
		assertFalse(n.hasNext());
	}

	@Test
	public void testMapOffline() throws IOException {
		ImmutableSequentialGraph g = new ErdosRenyiGraph(10, .5, 0, false);
		long[][] perm = Util.identity((long)g.numNodes());
		LongBigArrays.shuffle(perm, new XoRoShiRo128PlusRandom(0));
		long[][] inv = Util.invertPermutation(perm);
		ImmutableGraph gm = Transform.mapOffline(ImmutableGraph.wrap(new ArrayListMutableGraph(g).immutableView()), perm, 100);
		assertEquals(gm, Transform.mapOffline(ImmutableGraph.wrap(g), perm, 100));
		assertEquals(ImmutableGraph.wrap(g), Transform.mapOffline(Transform.mapOffline(ImmutableGraph.wrap(g), perm, 100), inv, 100));
		assertEquals(gm, gm.copy());

		perm = Util.identity((long)g.numNodes());
		set(perm, length(perm) - 1, -1);
		gm = Transform.mapOffline(ImmutableGraph.wrap(new ArrayListMutableGraph(g).immutableView()), perm, 100);
		assertEquals(gm, Transform.mapOffline(ImmutableGraph.wrap(g), perm, 100));
		assertEquals(gm, gm.copy());

		perm = Util.identity((long)g.numNodes());
		LongBigArrays.shuffle(perm, new XoRoShiRo128PlusRandom(0));
		set(perm, 0, -1);
		set(perm, length(perm) / 2, -1);
		gm = Transform.mapOffline(ImmutableGraph.wrap(new ArrayListMutableGraph(g).immutableView()), perm, 100);
		assertEquals(gm, Transform.mapOffline(ImmutableGraph.wrap(g), perm, 100));
		assertEquals(gm, gm.copy());

		perm = Util.identity((long)g.numNodes());
		set(perm, 1, 0);
		set(perm, length(perm) - 2, length(perm) - 1);
		gm = Transform.mapOffline(ImmutableGraph.wrap(new ArrayListMutableGraph(g).immutableView()), perm, 100);
		assertEquals(gm, Transform.mapOffline(ImmutableGraph.wrap(g), perm, 100));
		assertEquals(gm, gm.copy());

		g = new ErdosRenyiGraph(1000, .2, 0, false);
		perm = Util.identity((long)g.numNodes());
		LongBigArrays.shuffle(perm, new XoRoShiRo128PlusRandom(0));
		inv = Util.invertPermutation(perm);
		gm = Transform.mapOffline(ImmutableGraph.wrap(new ArrayListMutableGraph(g).immutableView()), perm, 100);
		assertEquals(gm, Transform.mapOffline(ImmutableGraph.wrap(g), perm, 1000000));
		assertEquals(ImmutableGraph.wrap(g), Transform.mapOffline(Transform.mapOffline(ImmutableGraph.wrap(g), perm, 10000), inv, 10000));
		assertEquals(gm, gm.copy());

		perm = Util.identity((long)g.numNodes());
		LongBigArrays.shuffle(perm, new XoRoShiRo128PlusRandom(0));
		set(perm, 0, -1);
		set(perm, length(perm) / 2, -1);
		set(perm, length(perm) / 4, -1);
		set(perm, 3 * length(perm) / 4, -1);
		gm = Transform.mapOffline(ImmutableGraph.wrap(new ArrayListMutableGraph(g).immutableView()), perm, 100);
		assertEquals(gm, Transform.mapOffline(ImmutableGraph.wrap(g), perm, 1000000));
		assertEquals(gm, gm.copy());
	}

	@Test
	public void testSymmetrizeOffline() throws IOException {
		ImmutableGraph g = ImmutableGraph.wrap(new ErdosRenyiGraph(5, .5, 0, false));
		ImmutableGraph gs = Transform.symmetrizeOffline(g, 10);
		assertEquals(gs, Transform.symmetrizeOffline(g, 5));
		assertEquals(gs, Transform.symmetrizeOffline(Transform.symmetrizeOffline(g, 100), 5));
		assertEquals(gs, gs.copy());

		g = ImmutableGraph.wrap(new ErdosRenyiGraph(100, .5, 0, false));
		gs = Transform.symmetrizeOffline(g, 100);
		assertEquals(gs, Transform.symmetrizeOffline(g, 1000));
		assertEquals(gs, Transform.symmetrizeOffline(Transform.symmetrizeOffline(g, 100), 10000));
		assertEquals(gs, gs.copy());

		g = ImmutableGraph.wrap(new ErdosRenyiGraph(1000, .2, 0, false));
		gs = Transform.symmetrizeOffline(g, 1000);
		assertEquals(gs, Transform.symmetrizeOffline(g, 10000));
		assertEquals(gs, Transform.symmetrizeOffline(Transform.symmetrizeOffline(g, 10000), 100000));
		assertEquals(gs, gs.copy());
	}

	@Test
	public void testSimplifyOffline() throws IOException {
		ImmutableGraph g = ImmutableGraph.wrap(new ErdosRenyiGraph(5, .5, 0, false));
		ImmutableGraph gs = Transform.simplifyOffline(g, 10);
		assertEquals(gs, Transform.simplifyOffline(g, 5));
		assertEquals(gs, Transform.simplifyOffline(Transform.simplifyOffline(g, 100), 5));
		assertEquals(gs, gs.copy());

		g = ImmutableGraph.wrap(new ErdosRenyiGraph(100, .5, 0, false));
		gs = Transform.simplifyOffline(g, 100);
		assertEquals(gs, Transform.simplifyOffline(g, 1000));
		assertEquals(gs, Transform.simplifyOffline(Transform.simplifyOffline(g, 100), 10000));
		assertEquals(gs, gs.copy());

		g = ImmutableGraph.wrap(new ErdosRenyiGraph(1000, .2, 0, false));
		gs = Transform.simplifyOffline(g, 1000);
		assertEquals(gs, Transform.simplifyOffline(g, 10000));
		assertEquals(gs, Transform.simplifyOffline(Transform.simplifyOffline(g, 10000), 100000));
		assertEquals(gs, gs.copy());
	}

	@Test
	public void testBatchGraphSplit() throws IOException {
		ImmutableGraph g;
		File tempGraph;
		g = ImmutableGraph.wrap(new ErdosRenyiGraph(5, .5, 0, false));
		tempGraph = storeTempGraph(Transform.transposeOffline(g, 5));
		assertEquals(Transform.transposeOffline(g, 10000), ImmutableGraph.load(tempGraph.toString()));
		deleteGraph(tempGraph);

		g = ImmutableGraph.wrap(new ErdosRenyiGraph(100, .5, 0, false));
		tempGraph = storeTempGraph(Transform.transposeOffline(g, 100));
		assertEquals(Transform.transposeOffline(g, 10000), ImmutableGraph.load(tempGraph.toString()));
		deleteGraph(tempGraph);

		g = ImmutableGraph.wrap(new ErdosRenyiGraph(1000, .20, 0, false));
		tempGraph = storeTempGraph(Transform.transposeOffline(g, 10000));
		assertEquals(Transform.transposeOffline(g, 10000), ImmutableGraph.load(tempGraph.toString()));
		deleteGraph(tempGraph);
	}

	@Test
	public void testSimplify() throws IOException {
		ImmutableGraph g = ImmutableGraph.wrap(new ErdosRenyiGraph(5, .50, 0, false));
		ImmutableGraph gs = Transform.simplifyOffline(g, 100000);
		ImmutableGraph gt = Transform.transposeOffline(g, 100000);
		assertEquals(gs, Transform.simplify(g, gt));
		assertEquals(gs, Transform.simplifyOffline(Transform.simplifyOffline(g, 100), 5));
		assertEquals(gs, gs.copy());

		g = ImmutableGraph.wrap(new ErdosRenyiGraph(100, .50, 0, false));
		gs = Transform.simplifyOffline(g, 100000);
		gt = Transform.transposeOffline(g, 100000);
		assertEquals(gs, Transform.simplify(g, gt));
		assertEquals(gs, Transform.simplifyOffline(Transform.simplifyOffline(g, 100), 100));
		assertEquals(gs, gs.copy());

		g = ImmutableGraph.wrap(new ErdosRenyiGraph(1000, .20, 0, false));
		gs = Transform.simplifyOffline(g, 100000);
		gt = Transform.transposeOffline(g, 100000);
		assertEquals(gs, Transform.simplify(g, gt));
		assertEquals(gs, Transform.simplifyOffline(Transform.simplifyOffline(g, 10000), 10000));
		assertEquals(gs, gs.copy());
	}

	@Test
	public void testFilteredGraphSplit() throws IOException {
		ImmutableGraph g;
		File tempGraph;
		g = ImmutableGraph.wrap((new ArrayListMutableGraph(new ErdosRenyiGraph(5, .5, 0, false)).immutableView()));
		tempGraph = storeTempGraph(Transform.filterArcs(g, Transform.NO_LOOPS));
		assertEquals(Transform.filterArcs(g, Transform.NO_LOOPS), ImmutableGraph.load(tempGraph.toString()));
		deleteGraph(tempGraph);

		g = ImmutableGraph.wrap((new ArrayListMutableGraph(new ErdosRenyiGraph(100, .5, 0, false)).immutableView()));
		tempGraph = storeTempGraph(Transform.filterArcs(g, Transform.NO_LOOPS));
		assertEquals(Transform.filterArcs(g, Transform.NO_LOOPS), ImmutableGraph.load(tempGraph.toString()));
		deleteGraph(tempGraph);

		g = ImmutableGraph.wrap((new ArrayListMutableGraph(new ErdosRenyiGraph(1000, .20, 0, false)).immutableView()));
		tempGraph = storeTempGraph(Transform.filterArcs(g, Transform.NO_LOOPS));
		assertEquals(Transform.filterArcs(g, Transform.NO_LOOPS), ImmutableGraph.load(tempGraph.toString()));
		deleteGraph(tempGraph);
	}

	@Test
	public void testArcLabelledFilteredGraphSplit() throws IOException {
		// Graph (x,Math.min(x+1,99),1 mod 4),...(x,Math.min(x+k,99),k mod 4) with x=0..99 and k=5
		final ArrayList<int[]> arcs = new ArrayList<>();
		for (int x = 0; x < 100; x++)
			for (int i = 1; i <= Math.min(5, 99 - x); i++)
				arcs.add(new int[] { x, x + i, i % 4 });
		arcs.add(new int[] { 99, 98, 1 }); // Needed to avoid cutting the graph short
		final ArrayList<int[]> arcsFiltered = new ArrayList<>();
		for (int x = 0; x < 100; x++)
			for (int i = 1; i <= Math.min(5, 99 - x); i++)
				if (i % 4 != 3 && x % 5 == 4 && (x + i) % 3 == 2)
					arcsFiltered.add(new int[] { x, x + i, i % 4 });
		arcsFiltered.add(new int[] { 99, 98, 1 }); // Needed to avoid cutting the graph short
		final int[][] result = new int[arcs.size()][];
		arcs.toArray(result);
		final int[][] resultFiltered = new int[arcsFiltered.size()][];
		arcsFiltered.toArray(resultFiltered);
		final File file = BitStreamArcLabelledGraphTest.storeTempGraph(new IntegerTriplesArcLabelledImmutableGraph(result));
		final ArcLabelledImmutableGraph graph = ArcLabelledImmutableGraph.load(file.toString());
		final File filteredFile = BitStreamArcLabelledGraphTest.storeTempGraph(new IntegerTriplesArcLabelledImmutableGraph(resultFiltered));
		final ArcLabelledImmutableGraph filteredGraph = ArcLabelledImmutableGraph.load(filteredFile.toString());
		final ArcLabelledImmutableGraph transformed = Transform.filterArcs(graph, (LabelledArcFilter)(i, j, label) -> label.getInt() % 4 != 3 && i % 5 == 4 && j % 3 == 2);
		assertEquals(filteredGraph, transformed);
		assertGraph(transformed);
	}

}
