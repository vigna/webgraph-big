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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.UnflaggedOption;

import it.unimi.dsi.big.webgraph.ImmutableGraph;
import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.big.webgraph.NodeIterator;
import it.unimi.dsi.logging.ProgressLogger;

/** Outputs (onto stdout) the graph adjacency matrix in SMAT format.
 *  The SMAT format is as follows:
 *
 *  <pre>
 *    n_nodes n_nodes n_arcs
 *    from to 1
 *    from to 1
 *    ...
 *  </pre>
 */


public class AdjMatrixSmatOut {
	@SuppressWarnings("unused")
	private static final boolean DEBUG = false;
	private static final Logger LOGGER = LoggerFactory.getLogger(AdjMatrixSmatOut.class);


	@SuppressWarnings("boxing")
	public static void main(final String arg[]) throws IOException, JSAPException {
		final SimpleJSAP jsap = new SimpleJSAP(AdjMatrixSmatOut.class.getName(),
				"Outputs the adjacency matrix of a given graph in SMAT format. ",
				new Parameter[] {
			new FlaggedOption("scale", JSAP.DOUBLE_PARSER, "1.0", JSAP.NOT_REQUIRED, 's', "scale", "Scale (e.g.: 0.5 produces a matrix of half the size of the original)."),
			new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the graph."),
		}
		);

		final JSAPResult jsapResult = jsap.parse(arg);
		if (jsap.messagePrinted()) return;

		final String basename = jsapResult.getString("basename");
		final ImmutableGraph graph = ImmutableGraph.loadOffline(basename);
		final long n = graph.numNodes();

		final ProgressLogger pl = new ProgressLogger(LOGGER);
		pl.expectedUpdates = n;
		pl.itemsName = "nodes";

		final double scale = jsapResult.getDouble("scale");

		if (scale == 1.0) {
			System.out.printf("%d %d %d\n", n, n, graph.numArcs());

			final NodeIterator nodeIterator = graph.nodeIterator();

			while (nodeIterator.hasNext()) {
				pl.update();
				final long x = nodeIterator.nextLong();
				long y;
				final LazyLongIterator successors = nodeIterator.successors();
				while ((y = successors.nextLong()) >= 0) {
					System.out.printf("%d %d 1\n", x, y);
				}
			}
		} else {
			final int realN = (int)(n * scale);
			final int mat[][] = new int[realN + 1][realN + 1];
			final NodeIterator nodeIterator = graph.nodeIterator();
			while (nodeIterator.hasNext()) {
				pl.update();
				final long x = nodeIterator.nextLong();
				long y;
				final LazyLongIterator successors = nodeIterator.successors();
				while ((y = successors.nextLong()) >= 0) {
					mat[(int)(x * scale)][(int)(y *scale)]++;
				}
			}
			int cnz=0;
			final double realNSquare = realN * realN;
			for (int x = 0; x < realN; x++)
				for (int y = 0; y < realN; y++)
					if (mat[x][y] > 0) cnz++;

			System.out.printf("%d %d %d\n", realN, realN, cnz);
			for (int x = 0; x < realN; x++)
				for (int y = 0; y < realN; y++)
					if (mat[x][y] > 0) System.out.printf("%d %d %f\n", x, y, mat[x][y] / realNSquare);

		}

	}
}
