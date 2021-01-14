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

package it.unimi.dsi.big.webgraph.scratch;

import java.io.IOException;

import it.unimi.dsi.big.webgraph.ImmutableGraph;
import it.unimi.dsi.big.webgraph.LazyLongIterator;

public class ComputeClassSizes {
	@SuppressWarnings("unused")
	private static final boolean DEBUG = false;
	public static void main(final String arg[]) throws IOException {
		final ImmutableGraph g = ImmutableGraph.load(arg[0]);
		final long n = g.numNodes();
		long r, s, c = 1;
		LazyLongIterator a, b;
		for(long i = 1; i < n; i++) {
			a = g.successors(i - 1);
			b = g.successors(i);
			while((r = a.nextLong()) == (s = b.nextLong()) && r != -1);
			if (s == r) c++;
			else {
				System.out.println(c);
				c = 1;
			}
		}

		System.out.println(c);
	}
}
