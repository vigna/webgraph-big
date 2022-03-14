/*
 * Copyright (C) 2008-2022 Paolo Boldi and Sebastiano Vigna
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
import java.util.Date;

import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.Switch;
import com.martiansoftware.jsap.UnflaggedOption;

import it.unimi.dsi.big.webgraph.ImmutableGraph;
import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.big.webgraph.NodeIterator;

/** Outputs (onto stdout) an EPS image showing a sketch of the graph adjacency matrix.
 */


public class AdjMatrixPSOut {
	@SuppressWarnings("unused")
	private static final boolean DEBUG = false;
	@SuppressWarnings("boxing")
	public static void main(final String arg[]) throws IOException, JSAPException {
		final SimpleJSAP jsap = new SimpleJSAP(AdjMatrixPSOut.class.getName(),
				"Outputs the adjacency matrix of a given graph as an EPS image. " +
				"Apart for the border, the image is an <var>n</var> x <var>n</var> square (<var>n</var> being the number of nodes), "+
				"and point (<var>x</var>,<var>y</var>) is set iff there is an arc from <var>x</var> to <var>y</var>. "+
				"(0,0) is the upper-left corner.",
				new Parameter[] {
			new Switch("border", 'b', "border", "Border should be output."),
			new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the graph."),
		}
		);

		final JSAPResult jsapResult = jsap.parse(arg);
		if (jsap.messagePrinted()) return;

		final String basename = jsapResult.getString("basename");
		final ImmutableGraph graph = ImmutableGraph.loadOffline(basename);
		final long n = graph.numNodes();
		final boolean border = jsapResult.getBoolean("border");

		System.out.printf("%%!PS-Adobe-3.0 EPSF-3.0\n%%%%Creator: Webgraph\n%%%%Title: Webgraph\n%%%%CreationDate: %TD\n%%%%DocumentData: Clean7Bit\n%%%%Origin: 0 0\n%%%%BoundingBox: 0 0 %d %d\n%%%%LanguageLevel: 2 \n%%%%Pages: 1\n%%%%Page: 1 1\n",
				new Date(),
				border? n + 1 : n - 1,
				border? n + 1 : n - 1);

		if (border)
			System.out.printf("newpath\n0 0 moveto\n0 %d lineto\n%d %d lineto\n%d 0 lineto\n0 0 lineto\nstroke\n",
					n + 1, n + 1, n + 1, n + 1);
		final NodeIterator nodeIterator = graph.nodeIterator();

		while (nodeIterator.hasNext()) {
			final long x = nodeIterator.nextLong();
			long y;
			final LazyLongIterator successors = nodeIterator.successors();
			while ((y = successors.nextLong()) >= 0) {
				System.out.printf("%d %d moveto\n%d %d lineto\n",
						border? x + 1 : x,
						border? n - y : n - 1 - y,
						border? x + 2 : x + 1,
						border? n - y + 1 : n - y);
			}
		}
		System.out.printf("stroke\n%%%%EOF\n");

	}
}
