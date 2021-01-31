/*
 * Copyright (C) 2020-2021 Paolo Boldi and Sebastiano Vigna
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

import java.io.IOException;

import it.unimi.dsi.big.webgraph.ImmutableGraph;
import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.big.webgraph.NodeIterator;

/**
 * A graph with typed nodes.
 *
 * Every node in a typed graph is identified by a long in which the upper bits store the type (at
 * most 2<sup>48</sup> types are allowed) and the lower bits store the id within the type (at most
 * 2<sup>48</sup> ids are allowed). The methods {@link #node(int, long)}, {@link #type(long)} and
 * {@link #id(long)} make conversions easy.
 */

public interface TypedGraph {

	public static final int ID_BITS = 48;

	public ImmutableGraph typeGraph();

	public long numNodes();

	public long numNodes(int type);

	public long numArcs();

	public long outdegree(long node) throws IOException;

	public LazyLongIterator successors(long node) throws IOException;

	public NodeIterator nodeIterator();

	public static int type(final long node) {
		return (int)(node >> ID_BITS);
	}

	public static long id(final long node) {
		return node & ((1L << ID_BITS) - 1);
	}

	public static long node(final int type, final long id) {
		return (long)type << ID_BITS | id;
	}
}
