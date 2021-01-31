/*
 * Copyright (C) 2007-2021 Sebastiano Vigna
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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Date;

import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;

/** Outputs (onto stdout) an EPS image showing a sketch of the graph adjacency matrix.
 */


public class SmatToPS {
	@SuppressWarnings("unused")
	private static final boolean DEBUG = false;
	@SuppressWarnings("boxing")
	public static void main(final String arg[]) throws IOException, JSAPException {
		final SimpleJSAP jsap = new SimpleJSAP(SmatToPS.class.getName(),
				"Outputs a given sparse SQUARE matrix (in SMAT format) to an EPS image.",
				new Parameter[] {
		}
		);

		@SuppressWarnings("unused")
		final
		JSAPResult jsapResult = jsap.parse(arg);
		if (jsap.messagePrinted()) return;


		final BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		String s[] = br.readLine().split(" "), line;
		final int n = Integer.parseInt(s[0]);

		System.out.printf("%%!PS-Adobe-3.0 EPSF-3.0\n%%%%Creator: Webgraph\n%%%%Title: Webgraph\n%%%%CreationDate: %TD\n%%%%DocumentData: Clean7Bit\n%%%%Origin: 0 0\n%%%%BoundingBox: 0 0 %d %d\n%%%%LanguageLevel: 2 \n%%%%Pages: 1\n%%%%Page: 1 1\n",
				new Date(),
				n,
				n);


		while ((line = br.readLine()) != null) {
			s = line.split(" ");
			final int x = Integer.parseInt(s[0]);
			final int y = Integer.parseInt(s[1]);
			final double g = Double.parseDouble(s[2]);
			System.out.printf("newpath\n%d %d moveto\n%d %d lineto\n%d %d lineto\n%d %d lineto\nclosepath\n%.3f setgray\nfill\n",
					y, n - x - 1, y + 1, n - x - 1, y + 1, n - x, y, n - x,
					(g >= 1 ? 0.0 : Math.exp(-3000 * ((1.0 / (1 - g) - 1)))));
		}
		System.out.printf("%%%%EOF\n");

	}
}
