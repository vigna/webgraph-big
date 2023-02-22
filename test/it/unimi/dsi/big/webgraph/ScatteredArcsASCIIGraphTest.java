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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.junit.Test;

import it.unimi.dsi.fastutil.BigArrays;
import it.unimi.dsi.fastutil.bytes.ByteArrays;
import it.unimi.dsi.fastutil.io.FastByteArrayInputStream;
import it.unimi.dsi.fastutil.objects.Object2LongArrayMap;
import it.unimi.dsi.fastutil.objects.Object2LongFunction;
import it.unimi.dsi.fastutil.objects.Object2LongOpenCustomHashMap;
import it.unimi.dsi.webgraph.ArrayListMutableGraph;
import it.unimi.dsi.webgraph.Transform;
import it.unimi.dsi.webgraph.examples.ErdosRenyiGraph;

public class ScatteredArcsASCIIGraphTest extends WebGraphTestCase {

	@Test
	public void testConstructor() throws UnsupportedEncodingException, IOException {

		ScatteredArcsASCIIGraph g = new ScatteredArcsASCIIGraph(new FastByteArrayInputStream("0 1\n0 2\n1 0\n1 2\n2 0\n2 1".getBytes("ASCII")));
		assertEquals(ArrayListMutableGraph.newCompleteGraph(3, false).immutableView(), new ArrayListMutableGraph(ImmutableGraph.wrap(g)).immutableView());
		assertArrayEquals(BigArrays.wrap(new long[] { 0, 1, 2 }), g.ids);

		g = new ScatteredArcsASCIIGraph(new FastByteArrayInputStream("-1 15\n15 2\n2 -1\nOOPS!\n-1 2".getBytes("ASCII")));
		assertEquals(new ArrayListMutableGraph(3, new int[][] {{0,1},{0,2},{1,2},{2,0}}).immutableView(), new ArrayListMutableGraph(ImmutableGraph.wrap(g)).immutableView());
		assertArrayEquals(BigArrays.wrap(new long[] { -1, 15, 2 }), g.ids);

		g = new ScatteredArcsASCIIGraph(new FastByteArrayInputStream("0 1\n0 2\n1  0\n1 \t 2\n2 0\n2 1".getBytes("ASCII")));
		assertEquals(ArrayListMutableGraph.newCompleteGraph(3, false).immutableView(), new ArrayListMutableGraph(ImmutableGraph.wrap(g)).immutableView());
		assertArrayEquals(BigArrays.wrap(new long[] { 0, 1, 2 }), g.ids);

		g = new ScatteredArcsASCIIGraph(new FastByteArrayInputStream("2 0\n2 1".getBytes("ASCII")));
		assertEquals(new ArrayListMutableGraph(3, new int[][] {{0,1},{0,2}}).immutableView(), new ArrayListMutableGraph(ImmutableGraph.wrap(g)).immutableView());
		assertArrayEquals(BigArrays.wrap(new long[] { 2, 0, 1 }), g.ids);

		g = new ScatteredArcsASCIIGraph(new FastByteArrayInputStream("1 2".getBytes("ASCII")));
		assertEquals(new ArrayListMutableGraph(2, new int[][] {{0,1}}).immutableView(), new ArrayListMutableGraph(ImmutableGraph.wrap(g)).immutableView());
		assertArrayEquals(BigArrays.wrap(new long[] { 1, 2 }), g.ids);

		g = new ScatteredArcsASCIIGraph(new FastByteArrayInputStream("2 1".getBytes("ASCII")));
		assertEquals(new ArrayListMutableGraph(2, new int[][] {{0,1}}).immutableView(), new ArrayListMutableGraph(ImmutableGraph.wrap(g)).immutableView());
		assertArrayEquals(BigArrays.wrap(new long[] { 2, 1 }), g.ids);

		g = new ScatteredArcsASCIIGraph(new FastByteArrayInputStream("0 1\n2 1".getBytes("ASCII")));
		assertEquals(new ArrayListMutableGraph(3, new int[][] {{0,1},{2,1}}).immutableView(), new ArrayListMutableGraph(ImmutableGraph.wrap(g)).immutableView());
		assertArrayEquals(BigArrays.wrap(new long[] { 0, 1, 2 }), g.ids);

		g = new ScatteredArcsASCIIGraph(new FastByteArrayInputStream("\n0 1\n\n2 1".getBytes("ASCII")));
		assertEquals(new ArrayListMutableGraph(3, new int[][] {{0,1},{2,1}}).immutableView(), new ArrayListMutableGraph(ImmutableGraph.wrap(g)).immutableView());
		assertArrayEquals(BigArrays.wrap(new long[] { 0, 1, 2 }), g.ids);

		g = new ScatteredArcsASCIIGraph(new FastByteArrayInputStream("\n0 1\n# comment\n2\n2 1\n2 X".getBytes("ASCII")));
		assertEquals(new ArrayListMutableGraph(3, new int[][] {{0,1},{2,1}}).immutableView(), new ArrayListMutableGraph(ImmutableGraph.wrap(g)).immutableView());
		assertArrayEquals(BigArrays.wrap(new long[] { 0, 1, 2 }), g.ids);

		g = new ScatteredArcsASCIIGraph(new FastByteArrayInputStream("0 1\n0 2\n1 0\n1 2\n2 0\n2 1".getBytes("ASCII")), true, false, 1);
		assertEquals(ArrayListMutableGraph.newCompleteGraph(3, false).immutableView(), new ArrayListMutableGraph(ImmutableGraph.wrap(g)).immutableView());
		assertArrayEquals(BigArrays.wrap(new long[] { 0, 1, 2 }), g.ids);

		g = new ScatteredArcsASCIIGraph(new FastByteArrayInputStream("0 1\n0 2\n1  0\n1 \t 2\n2 0\n2 1".getBytes("ASCII")), true, false, 1);
		assertEquals(ArrayListMutableGraph.newCompleteGraph(3, false).immutableView(), new ArrayListMutableGraph(ImmutableGraph.wrap(g)).immutableView());
		assertArrayEquals(BigArrays.wrap(new long[] { 0, 1, 2 }), g.ids);

		g = new ScatteredArcsASCIIGraph(new FastByteArrayInputStream("2 0\n2 1".getBytes("ASCII")), true, false, 1);
		assertEquals(Transform.symmetrize(new ArrayListMutableGraph(3, new int[][] {{0,1},{0,2}}).immutableView()), new ArrayListMutableGraph(ImmutableGraph.wrap(g)).immutableView());
		assertArrayEquals(BigArrays.wrap(new long[] { 2, 0, 1 }), g.ids);

		g = new ScatteredArcsASCIIGraph(new FastByteArrayInputStream("1 2".getBytes("ASCII")), true, false, 1);
		assertEquals(Transform.symmetrize(new ArrayListMutableGraph(2, new int[][] {{0,1}}).immutableView()), new ArrayListMutableGraph(ImmutableGraph.wrap(g)).immutableView());
		assertArrayEquals(BigArrays.wrap(new long[] { 1, 2 }), g.ids);

		g = new ScatteredArcsASCIIGraph(new FastByteArrayInputStream("2 1".getBytes("ASCII")), true, false, 1);
		assertEquals(Transform.symmetrize(new ArrayListMutableGraph(2, new int[][] {{0,1}}).immutableView()), new ArrayListMutableGraph(ImmutableGraph.wrap(g)).immutableView());
		assertArrayEquals(BigArrays.wrap(new long[] { 2, 1 }), g.ids);

		g = new ScatteredArcsASCIIGraph(new FastByteArrayInputStream("0 1\n2 1".getBytes("ASCII")), true, false, 1);
		assertEquals(Transform.symmetrize(new ArrayListMutableGraph(3, new int[][] {{0,1},{2,1}}).immutableView()), new ArrayListMutableGraph(ImmutableGraph.wrap(g)).immutableView());
		assertArrayEquals(BigArrays.wrap(new long[] { 0, 1, 2 }), g.ids);

		g = new ScatteredArcsASCIIGraph(new FastByteArrayInputStream("\n0 1\n\n2 1".getBytes("ASCII")), true, false, 1);
		assertEquals(Transform.symmetrize(new ArrayListMutableGraph(3, new int[][] {{0,1},{2,1}}).immutableView()), new ArrayListMutableGraph(ImmutableGraph.wrap(g)).immutableView());
		assertArrayEquals(BigArrays.wrap(new long[] { 0, 1, 2 }), g.ids);

		g = new ScatteredArcsASCIIGraph(new FastByteArrayInputStream("\n0 1\n# comment\n2\n2 1\n2 X".getBytes("ASCII")), true, false, 1);
		assertEquals(Transform.symmetrize(new ArrayListMutableGraph(3, new int[][] {{0,1},{2,1}}).immutableView()), new ArrayListMutableGraph(ImmutableGraph.wrap(g)).immutableView());
		assertArrayEquals(BigArrays.wrap(new long[] { 0, 1, 2 }), g.ids);

		g = new ScatteredArcsASCIIGraph(new FastByteArrayInputStream("0 0\n0 1\n0 2\n2 2\n1 0\n1 2\n2 0\n2 1".getBytes("ASCII")), true, true, 2);
		assertEquals(ArrayListMutableGraph.newCompleteGraph(3, false).immutableView(), new ArrayListMutableGraph(ImmutableGraph.wrap(g)).immutableView());
		assertArrayEquals(BigArrays.wrap(new long[] { 0, 1, 2 }), g.ids);

	}


	@Test
	public void testConstructorWithStrings() throws UnsupportedEncodingException, IOException {
		final Object2LongFunction<String> map = new Object2LongArrayMap<>();
		map.defaultReturnValue(-1);

		map.clear();
		map.put("0", 0);
		map.put("1", 1);
		map.put("2", 2);
		assertEquals(ImmutableGraph.wrap(ArrayListMutableGraph.newCompleteGraph(3, false).immutableView()), new ScatteredArcsASCIIGraph(new FastByteArrayInputStream("0 1\n0 2\n1 0\n1 2\n2 0\n2 1".getBytes("ASCII")), map, null, 3));

		map.clear();
		map.put("-1", 1);
		map.put("15", 0);
		map.put("2", 2);
		final ImmutableGraph g = ImmutableGraph.wrap(new ArrayListMutableGraph(3, new int[][] {{0,2},{1,0},{1,2},{2,1}}).immutableView());
		assertEquals(g, new ScatteredArcsASCIIGraph(new FastByteArrayInputStream("-1 15\n15 2\n2 -1\nOOPS!\n-1 2".getBytes("ASCII")), map, null, 3));
		assertEquals(g, new ScatteredArcsASCIIGraph(new FastByteArrayInputStream("-1 15\n15 2\n2 -1\nOOPS!\n-1 2\n32 2\n2 32".getBytes("ASCII")), map, null, 3));
	}

	@Test
	public void testByteArrays() throws UnsupportedEncodingException, IOException {
		final Object2LongFunction<byte[]> map = new Object2LongOpenCustomHashMap<>(ByteArrays.HASH_STRATEGY);
		map.defaultReturnValue(-1);

		map.clear();
		map.put("0".getBytes(), 0);
		map.put("1".getBytes(), 1);
		map.put("2".getBytes(), 2);
		assertEquals(ImmutableGraph.wrap(ArrayListMutableGraph.newCompleteGraph(3, false).immutableView()), new ScatteredArcsASCIIGraph(new FastByteArrayInputStream("0 1\n0 2\n1 0\n1 2\n2 0\n2 1".getBytes("ASCII")), map, 3));

		map.clear();
		map.put("-1".getBytes(), 1);
		map.put("15".getBytes(), 0);
		map.put("2".getBytes(), 2);
		final ImmutableGraph g = ImmutableGraph.wrap(new ArrayListMutableGraph(3, new int[][] { { 0, 2 }, { 1, 0 },
				{ 1, 2 }, { 2, 1 } }).immutableView());
		assertEquals(g, new ScatteredArcsASCIIGraph(new FastByteArrayInputStream("-1 15\n15 2\n2 -1\nOOPS!\n-1 2".getBytes("ASCII")), map, 3));
		assertEquals(g, new ScatteredArcsASCIIGraph(new FastByteArrayInputStream("-1 15\n15 2\n2 -1\nOOPS!\n-1 2\n32 2\n2 32".getBytes("ASCII")), map, 3));
	}

	@Test(expected=IllegalArgumentException.class)
	public void testTargetOutOfRange() throws UnsupportedEncodingException, IOException {
		final Object2LongFunction<String> map = new Object2LongArrayMap<>();
		map.defaultReturnValue(-1);
		map.put("0", 0);
		map.put("1", 1);
		map.put("2", 2);
		assertEquals(ImmutableGraph.wrap(ArrayListMutableGraph.newCompleteGraph(3, false).immutableView()), new ScatteredArcsASCIIGraph(new FastByteArrayInputStream("0 1\n0 2".getBytes("ASCII")), map, null, 2));
	}

	@Test(expected=IllegalArgumentException.class)
	public void testSourceOutOfRange() throws UnsupportedEncodingException, IOException {
		final Object2LongFunction<String> map = new Object2LongArrayMap<>();
		map.defaultReturnValue(-1);
		map.put("0", 0);
		map.put("1", 1);
		map.put("2", 2);
		assertEquals(ImmutableGraph.wrap(ArrayListMutableGraph.newCompleteGraph(3, false).immutableView()), new ScatteredArcsASCIIGraph(new FastByteArrayInputStream("0 1\n2 0".getBytes("ASCII")), map, null, 2));
	}

	@Test
	public void testLarge() throws IOException {
		final ErdosRenyiGraph erdosRenyiGraph = new ErdosRenyiGraph(100000, .0005, 0, true);
		final ImmutableGraph bigErdosRenyiGraph = ImmutableGraph.wrap(erdosRenyiGraph);
		final StringBuilder b = new StringBuilder();
		for (final NodeIterator nodeIterator = bigErdosRenyiGraph.nodeIterator(); nodeIterator.hasNext();) {
			final int curr = (int)nodeIterator.nextLong();
			final LazyLongIterator successors = nodeIterator.successors();
			for (long s; (s = successors.nextLong()) != -1;) b.append(-curr).append('\t').append(-s).append('\n');
		}

		final ScatteredArcsASCIIGraph g = new ScatteredArcsASCIIGraph(new FastByteArrayInputStream(b.toString().getBytes("ASCII")), false, false, 10000, null, null);
		final int[] perm = new int[(int)g.numNodes()];
		for (int i = 0; i < g.numNodes(); i++) perm[i] = (int)-BigArrays.get(g.ids, i);

		assertEquals(erdosRenyiGraph, Transform.map(new ArrayListMutableGraph(ImmutableGraph.wrap(g)).immutableView(), perm));
	}
}
