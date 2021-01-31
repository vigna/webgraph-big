/*
 * Copyright (C) 2006-2021 Sebastiano Vigna
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

package it.unimi.dsi.big.webgraph.examples;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.NoSuchElementException;

import it.unimi.dsi.big.webgraph.ImmutableGraph;
import it.unimi.dsi.big.webgraph.ImmutableSequentialGraph;
import it.unimi.dsi.big.webgraph.NodeIterator;
import it.unimi.dsi.fastutil.BigArrays;
import it.unimi.dsi.fastutil.longs.LongBigArrays;
import it.unimi.dsi.logging.ProgressLogger;


/** Exposes a graph in a simple binary format as an (offline-only) {@link ImmutableGraph}.
 *
 * <P>This class is a simple example that should help in understanding how to interface
 * WebGraph with external data. We have a graph contained in a file and represented by a list of binary
 * 32-bit integers as follows:
 * first we have the number of nodes, then the number of successors of node 0, then the list in increasing
 * order of successors of node 0, then the number of successors of node 1, then the list in increasing
 * order of successors of node 1, and so on.
 *
 * <P>If we want to transform this graph into, say, a {@link it.unimi.dsi.big.webgraph.BVGraph},
 * we must create a class that exposes the file as an {@link it.unimi.dsi.big.webgraph.ImmutableGraph}
 * and than save it using {@link it.unimi.dsi.big.webgraph.BVGraph#store(ImmutableGraph,CharSequence)} or by calling
 * the main method of {@link it.unimi.dsi.big.webgraph.BVGraph}.
 * A complete implementation is not necessary, as {@link it.unimi.dsi.big.webgraph.BVGraph} uses
 * just {@link #nodeIterator()}. Since we are just interesting in importing data, we do not
 * implement efficient random access methods, and the only loading method we implement is {@link #loadOffline(CharSequence)}.
 */

public class IntegerListImmutableGraph extends ImmutableSequentialGraph {

	/** The filename of the graph. */
	final private String filename;
	/** The number of nodes, read at creation time and cached. */
	final private long numNodes;

	private IntegerListImmutableGraph(final CharSequence filename) throws IOException {
		this.filename = filename.toString();
		final DataInputStream dis = new DataInputStream(new FileInputStream(this.filename));
		numNodes = dis.readLong();
		dis.close();
	}

	@Override
	public long numNodes() {
		return numNodes;
	}

	@Override
	public NodeIterator nodeIterator() {
		try {
			return new NodeIterator() {
				final long n = numNodes();
				final DataInputStream dis = new DataInputStream(new FileInputStream(IntegerListImmutableGraph.this.filename));
				long curr = - 1, outdegree;
				long[][] successorsArray = LongBigArrays.EMPTY_BIG_ARRAY;

				{
					try {
						dis.readInt(); // Skip number of nodes
					}
					catch(final IOException e) {
						throw new RuntimeException(e);
					}
				}

				@Override
				public long nextLong() {
					if (! hasNext()) throw new NoSuchElementException();
					try {
						outdegree = dis.readLong();
					}
					catch (final IOException e) {
						throw new RuntimeException(e);
					}
					return ++curr;
				}

				@Override
				public boolean hasNext() {
					return (curr < n - 1);
				}

				@Override
				public long[][] successorBigArray() {
					if (curr == - 1) throw new IllegalStateException();
					successorsArray = BigArrays.ensureCapacity(successorsArray, outdegree, 0);
					try {
						for (long i = 0; i < outdegree; i++) BigArrays.set(successorsArray, i, dis.readLong());
					}
					catch (final IOException e) {
						throw new RuntimeException(e);
					}
					return successorsArray;
				}

				@Override
				public long outdegree() {
					if (curr == - 1) throw new IllegalStateException();
					return outdegree;
				}

				@SuppressWarnings("deprecation")
				@Override
				protected void finalize() throws Throwable {
					try {
						dis.close();
					}
					finally {
						super.finalize();
					}
				}
			};
		}
		catch (final FileNotFoundException e) {
			throw new RuntimeException(e);
		}
	}

	public static ImmutableGraph load(final CharSequence basename, final ProgressLogger pl) {
		throw new UnsupportedOperationException("Graphs may be loaded offline only");
	}

	public static ImmutableGraph load(final CharSequence basename) {
		return load(basename, (ProgressLogger)null);
	}

	@Deprecated
	public static ImmutableGraph loadSequential(final CharSequence basename, final ProgressLogger pl) {
		return load(basename, pl);
	}

	@Deprecated
	public static ImmutableGraph loadSequential(final CharSequence basename) {
		return load(basename, (ProgressLogger)null);
	}

	public static ImmutableGraph loadOffline(final CharSequence basename, final ProgressLogger pl) throws IOException {
		return new IntegerListImmutableGraph(basename);
	}

	public static ImmutableGraph loadOffline(final CharSequence basename) throws IOException {
		return loadOffline(basename, (ProgressLogger)null);
	}
}
