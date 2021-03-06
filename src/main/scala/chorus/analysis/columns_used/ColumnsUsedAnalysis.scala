/*
 * Copyright (c) 2017 Uber Technologies, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package chorus.analysis.columns_used
import chorus.dataflow.column.DataflowGraphColumnAnalysis
import chorus.dataflow.domain.SetDomain
import chorus.sql.dataflow_graph.relation.DataTable

/** Returns a set of all data table columns influencing each output column.
  */
class ColumnsUsedAnalysis extends DataflowGraphColumnAnalysis(new SetDomain[String]) {
  override def transferDataTable(d: DataTable, idx: Int, fact: Set[String]): Set[String] = {
    val qualifiedColName = s"${d.name}.${d.getColumnName(idx)}"
    fact ++ Set(qualifiedColName)
  }
}
