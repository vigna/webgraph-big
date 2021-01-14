/*
 * Copyright (C) 2020 Paolo Boldi and Sebastiano Vigna
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

package it.unimi.dsi.big.webgraph.typed;

import static it.unimi.dsi.big.webgraph.typed.TypedGraph.node;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import org.apache.commons.configuration2.ex.ConfigurationException;
import org.junit.Test;

import it.unimi.dsi.big.webgraph.BVGraph;
import it.unimi.dsi.big.webgraph.ImmutableGraph;
import it.unimi.dsi.big.webgraph.LazyLongIterators;
import it.unimi.dsi.big.webgraph.NodeIterator;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.longs.LongBigArrayBigList;
import it.unimi.dsi.webgraph.ArrayListMutableGraph;

public class ConvertToTypedGraphTest {

	@Test
	public void testSmall() throws IOException, ClassNotFoundException, ConfigurationException {
		final ImmutableGraph graph = ImmutableGraph.wrap(new ArrayListMutableGraph(5, new int[][] {
				{ 0, 1 }, { 1, 2 }, { 2, 0 }, { 3, 2 }, { 0, 3 } }).immutableView());
		final ImmutableGraph typeGraph = ImmutableGraph.wrap(new ArrayListMutableGraph(4, new int[][] { { 0, 0 },
				{ 0, 1 }, { 0, 2 }, { 1, 2 }, { 1, 0 }, { 2, 0 } }).immutableView());
		final LongBigArrayBigList nodeTypes = LongBigArrayBigList.wrap(new long[][] { { 0, 1, 2, 0, 3 } });
		final File basename = new File("/tmp/graphtest");// File.createTempFile(ConvertToTypedGraphTest.class.getName(),
															// "-test");
		final String typeBasename = basename.toString() + "-types";
		BVGraph.store(typeGraph, typeBasename);
		ConvertToTypedGraph.convert(graph, typeGraph, nodeTypes, basename.toString(), null);
		final BVImmutableTypedGraph typedGraph = BVImmutableTypedGraph.load(basename.toString(), typeBasename.toString(), null);

		long[] succ;

		long node = node(0, 0);
		succ = new long[(int)typedGraph.outdegree(node)];
		LazyLongIterators.unwrap(typedGraph.successors(node), succ);
		assertArrayEquals(new long[] { node(0, 1), node(1, 0) }, succ);

		node = node(1, 0);
		succ = new long[(int)typedGraph.outdegree(node)];
		LazyLongIterators.unwrap(typedGraph.successors(node), succ);
		assertArrayEquals(new long[] { node(2, 0) }, succ);

		node = node(2, 0);
		succ = new long[(int)typedGraph.outdegree(node)];
		LazyLongIterators.unwrap(typedGraph.successors(node), succ);
		assertArrayEquals(new long[] { node(0, 0) }, succ);

		node = node(0, 1);
		succ = new long[(int)typedGraph.outdegree(node)];
		LazyLongIterators.unwrap(typedGraph.successors(node), succ);
		assertArrayEquals(new long[] { node(2, 0) }, succ);

		final NodeIterator nodeIterator = typedGraph.nodeIterator();
		assertEquals(nodeIterator.nextLong(), node(0, 0));
		assertEquals(nodeIterator.nextLong(), node(0, 1));
		assertEquals(nodeIterator.nextLong(), node(1, 0));
		assertEquals(nodeIterator.nextLong(), node(2, 0));
		assertEquals(nodeIterator.nextLong(), node(3, 0));
		assertFalse(nodeIterator.hasNext());

		assertTrue(ConvertToTypedGraph.verify(graph, typeGraph, nodeTypes, BinIO.loadLongsBig(basename + ConvertToTypedGraph.IDS_EXTENSION), typedGraph, null));
	}
}
