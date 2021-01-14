/*
 * Copyright (C) 2009-2020 Sebastiano Vigna
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

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.Scanner;

import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.fastutil.objects.Object2LongFunction;

public class ConvertArcFile {
	@SuppressWarnings("unchecked")
	public static void main(final String arg[]) throws IOException, ClassNotFoundException {
		final Object2LongFunction<String> convert = (Object2LongFunction<String>)BinIO.loadObject(arg[0]);
		@SuppressWarnings("resource")
		final
		Scanner scanner = new Scanner(new FastBufferedInputStream(System.in));
		final PrintWriter pw = new PrintWriter(new BufferedWriter(new OutputStreamWriter(System.out)));
		scanner.useDelimiter("\t|\r|\n");
		while(scanner.hasNext()) {
			pw.print(convert.getLong(scanner.next()));
			pw.print('\t');
			pw.print(convert.getLong(scanner.next()));
			pw.println();
		}

		pw.close();
	}
}
