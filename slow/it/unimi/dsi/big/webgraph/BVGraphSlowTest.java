/*
 * Copyright (C) 2011-2020 Sebastiano Vigna
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

import java.io.File;
import java.io.IOException;
import java.util.NoSuchElementException;

import org.junit.Test;

public class BVGraphSlowTest extends WebGraphTestCase {

	protected final static class BigGraph extends ImmutableSequentialGraph {
		private final long numNodes;
		private final long outdegree;
		private final int step;

		public BigGraph(final long numNodes, final long outdegree, final int step) {
			if (outdegree * step > numNodes) throw new IllegalArgumentException();
			this.numNodes = numNodes;
			this.outdegree = outdegree;
			this.step = step;
		}

		public BigGraph(final long outdegree, final int step) {
			this(outdegree * step, outdegree, step);
		}

		@Override
		public long numNodes() {
			return numNodes;
		}

		@Override
		public NodeIterator nodeIterator(final long from) {
			return new NodeIterator() {
				long next = 0;
				@Override
				public boolean hasNext() {
					return next < numNodes();
				}

				@Override
				public long nextLong() {
					if (! hasNext()) throw new NoSuchElementException();
					return next++;
				}

				@Override
				public long outdegree() {
					return next < 2 ? outdegree : 2;
				}

				@Override
				public LazyLongIterator successors() {
					if (next >= 2) return LazyLongIterators.wrap(new long[] { next - 2, next - 1 });
					else return new AbstractLazyLongIterator() {
						public long i = 0;
						@Override
						public long nextLong() {
							if (i == outdegree) return -1;
							else return i++ * step;
						}
					};
				}
			};
		}
	}

	@Test
	public void testStore() throws IOException {
		final ImmutableGraph graph = new BigGraph(3L << 31, 1L << 30, 4);
		final File basename = File.createTempFile(BVGraphSlowTest.class.getSimpleName(), "test");
		BVGraph.store(graph, basename.toString());
		assertEquals(graph, BVGraph.load(basename.toString()));
		assertEquals(graph, BVGraph.loadMapped(basename.toString()));
		assertEquals(graph, BVGraph.loadOffline(basename.toString()));
		deleteGraph(basename);
	}
}
