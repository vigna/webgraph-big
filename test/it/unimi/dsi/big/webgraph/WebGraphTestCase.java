/*
 * Copyright (C) 2003-2022 Sebastiano Vigna
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

import static it.unimi.dsi.fastutil.BigArrays.get;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import it.unimi.dsi.big.webgraph.labelling.ArcLabelledImmutableGraph;
import it.unimi.dsi.big.webgraph.labelling.ArcLabelledNodeIterator;
import it.unimi.dsi.big.webgraph.labelling.ArcLabelledNodeIterator.LabelledArcIterator;
import it.unimi.dsi.big.webgraph.labelling.BitStreamArcLabelledImmutableGraph;
import it.unimi.dsi.big.webgraph.labelling.Label;

/** A JUnit test case providing additional assertions
 * for {@linkplain it.unimi.dsi.big.webgraph.ImmutableGraph immutable graphs}.
 */

public abstract class WebGraphTestCase {

	private static void copy(final InputStream in, final OutputStream out) throws IOException {
		int c;
		while((c = in.read()) != -1) out.write(c);
		out.close();
	}

	/** Returns a path to a temporary graph that copies a resource graph with given basename.
	 *
	 * @param basename the basename.
	 * @return the graph.
	 * @throws IOException
	 */
	public String getGraphPath(final String basename) throws IOException {
		final File file = File.createTempFile(getClass().getSimpleName(), "graph");
		file.delete();

		copy(getClass().getResourceAsStream(basename + BVGraph.GRAPH_EXTENSION), new FileOutputStream(file.getCanonicalPath() + BVGraph.GRAPH_EXTENSION));
		copy(getClass().getResourceAsStream(basename + BVGraph.OFFSETS_EXTENSION), new FileOutputStream(file.getCanonicalPath() + BVGraph.OFFSETS_EXTENSION));
		copy(getClass().getResourceAsStream(basename + BVGraph.PROPERTIES_EXTENSION), new FileOutputStream(file.getCanonicalPath() + BVGraph.PROPERTIES_EXTENSION));

		return file.getCanonicalPath();
	}

	/** Cleans up a temporary graph.
	 *
	 * @param basename the basename.
	 */

	public static void deleteGraph(final String basename) {
		deleteGraph(new File(basename));
	}


	/** Cleans up a temporary graph.
	 *
	 * @param basename the basename.
	 */
	public static void deleteGraph(final File basename) {
		new File(basename + BVGraph.GRAPH_EXTENSION).delete();
		new File(basename + BVGraph.OFFSETS_EXTENSION).delete();
		new File(basename + BVGraph.OFFSETS_BIG_LIST_EXTENSION).delete();
		new File(basename + ImmutableGraph.PROPERTIES_EXTENSION).delete();
	}

	/**
	 * Cleans up a temporary {@link BitStreamArcLabelledImmutableGraph}.
	 *
	 * @param basename the basename.
	 * @param basenameUnderlying the basename of the underlying graph.
	 */

	public static void deleteLabelledGraph(final String basename, final String basenameUnderlying) {
		BVGraphTest.deleteGraph(basenameUnderlying);
		new File(basename + ImmutableGraph.PROPERTIES_EXTENSION).delete();
		new File(basename + BitStreamArcLabelledImmutableGraph.LABELS_EXTENSION).delete();
		new File(basename + BitStreamArcLabelledImmutableGraph.LABEL_OFFSETS_EXTENSION).delete();
	}

	/** Performs a stress-test of an immutable graph. All available methods
	 * for accessing outdegrees and successors are cross-checked.
	 *
	 * @param g the immutable graph to be tested.
	 */

	public static void assertGraph(final ImmutableGraph g) {
		NodeIterator nodeIterator0 = g.nodeIterator(), nodeIterator1 = g.nodeIterator();
		long d;
		long[][] s0;
		Label[][] l0;
		LazyLongIterator s1;
		int m = 0;
		long curr;
		// Check that iterator and array methods return the same values in sequential scans.
		for(long i = g.numNodes(); i-- != 0;) {
			curr = nodeIterator0.nextLong();
			assertEquals(curr, nodeIterator1.nextLong());
			d = nodeIterator0.outdegree();
			m += d;
			assertEquals(d, nodeIterator1.outdegree());

			s0 = nodeIterator0.successorBigArray();
			s1 = nodeIterator1.successors();
			for (long k = 0; k < d; k++) assertEquals(get(s0, k), s1.nextLong());
			assertEquals(-1, s1.nextLong());

			if (g instanceof ArcLabelledImmutableGraph) {
				l0 = ((ArcLabelledNodeIterator)nodeIterator0).labelBigArray();
				s1 = ((ArcLabelledNodeIterator)nodeIterator1).successors();
				for(long k = 0; k < d; k++) {
					s1.nextLong();
					assertEquals(get(l0, k), ((LabelledArcIterator)s1).label());
				}
			}

			assertEquals(-1, s1.nextLong());
		}

		try {
			assertEquals(m, g.numArcs());
		}
		catch(final UnsupportedOperationException ignore) {} // A graph might not support numArcs().
		assertFalse(nodeIterator0.hasNext());
		assertFalse(nodeIterator1.hasNext());

		if (! g.randomAccess()) return;

		// Check that sequential iterator methods and random methods do coincide.
		String msg;

		for(long s = 0; s < g.numNodes() - 1; s++) {
			nodeIterator1 = g.nodeIterator(s);
			for(long i = g.numNodes() - s; i-- != 0;) {
				curr = nodeIterator1.nextLong();
				msg = "Node " + curr + ", starting from " + s + ":";
				d = g.outdegree(curr);
				assertEquals(msg, d, nodeIterator1.outdegree());
				s0 = g.successorBigArray(curr);
				s1 = nodeIterator1.successors();
				for (long k = 0; k < d; k++) assertEquals(msg, get(s0, k), s1.nextLong());
				s1 = g.successors(curr);
				for (long k = 0; k < d; k++) assertEquals(msg, get(s0, k), s1.nextLong());
				assertEquals(msg, -1, s1.nextLong());

				if (g instanceof ArcLabelledImmutableGraph) {
					l0 = ((ArcLabelledImmutableGraph)g).labelBigArray(curr);
					s1 = ((ArcLabelledNodeIterator)nodeIterator1).successors();
					for(long k = 0; k < d; k++) {
						s1.nextLong();
						assertEquals(msg, get(l0, k), ((LabelledArcIterator)s1).label());
					}
					s1 = g.successors(curr);
					for(long k = 0; k < d; k++) {
						s1.nextLong();
						assertEquals(msg, get(l0, k), ((LabelledArcIterator)s1).label());
					}
					assertEquals(msg, -1, s1.nextLong());
				}
			}
		}

		// Check that cross-access works.

		nodeIterator0 = g.nodeIterator();
		for(long s = 0; s < g.numNodes(); s++) {
			d = g.outdegree(s);
			nodeIterator0.nextLong();
			final LazyLongIterator successors = g.successors(s);
			final long[][] succ = nodeIterator0.successorBigArray();
			for(long i = 0; i < d; i++) {
				final long t = successors.nextLong();
				assertEquals(get(succ, i), t);
				g.outdegree(t);
			}

		}
		// Check copies
		assertEquals(g, g.copy());
	}
}
