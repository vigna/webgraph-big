/*
 * Copyright (C) 2008-2020 Sebastiano Vigna
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
import it.unimi.dsi.big.webgraph.NodeIterator;
import it.unimi.dsi.bits.Fast;

public class PrintDeltas {
	public static void main(final String arg[]) throws IOException {
		final ImmutableGraph g = ImmutableGraph.load(arg[0]);
		long d, p, s, x;
		LazyLongIterator a;
		final NodeIterator n = g.nodeIterator();
		while(n.hasNext()) {
			x = n.nextLong();
			a = n.successors();
			d = n.outdegree();
			p = -1;
			for(long i = 0; i < d; i++) {
				s = a.nextLong();
				if (p == -1) System.out.println(Fast.int2nat(s - x));
				else System.out.println(s - p - 1);
				p = s;
			}
		}
	}
}
