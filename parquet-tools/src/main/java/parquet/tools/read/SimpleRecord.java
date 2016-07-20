/**
 * Copyright 2013 ARRIS, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package parquet.tools.read;

import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.google.common.base.Strings;

public class SimpleRecord {
  public static final int TAB_SIZE = 2;
  private final List<NameValue> values;

  public SimpleRecord() {
    this.values = new ArrayList<NameValue>();
  }

  public void add(String name, Object value) {
    values.add(new NameValue(name,value));
  }
  
  public List<NameValue> getValues() {
    return Collections.unmodifiableList(values);
  }

  public String toString() {
    return values.toString();
  }

  public void prettyPrint() {
    prettyPrint(new PrintWriter(System.out,true));
  }

  public void prettyPrint(PrintWriter out) {
    prettyPrint(out, 0, false);
  }
  public void prettyPrint(PrintWriter out, boolean showTs) {
	prettyPrint(out, 0,  showTs);
  }

  public void prettyPrint(PrintWriter out, int depth,  boolean showTs) {
    for (NameValue value : values) {
      out.print(Strings.repeat(".", depth));

      out.print(value.getName());
      Object val = value.getValue();
      if (val == null) {
        out.print(" = ");
        out.print("<null>");
      } else if (byte[].class == val.getClass()) {
        out.print(" = ");
        byte[] barray = (byte[])val;
        if(!showTs || barray.length != 12)       
        	out.print(Arrays.toString(barray));
        else {
        	// it is likely to be int96 timestamp
        	ByteBuffer buff = ByteBuffer.wrap(barray);
			buff.order(ByteOrder.LITTLE_ENDIAN);
			long nanosTime = buff.getLong();
			int jday = buff.getInt();
			out.print(julianToGregorian(jday) +" " + daysInNanosToTime(nanosTime));
        }
      } else if (short[].class == val.getClass()) {
        out.print(" = ");
        out.print(Arrays.toString((short[])val));
      } else if (int[].class == val.getClass()) {
        out.print(" = ");
        out.print(Arrays.toString((int[])val));
      } else if (long[].class == val.getClass()) {
        out.print(" = ");
        out.print(Arrays.toString((long[])val));
      } else if (float[].class == val.getClass()) {
        out.print(" = ");
        out.print(Arrays.toString((float[])val));
      } else if (double[].class == val.getClass()) {
        out.print(" = ");
        out.print(Arrays.toString((double[])val));
      } else if (boolean[].class == val.getClass()) {
        out.print(" = ");
        out.print(Arrays.toString((boolean[])val));
      } else if (val.getClass().isArray()) {
        out.print(" = ");
        out.print(Arrays.deepToString((Object[])val));
      } else if (SimpleRecord.class.isAssignableFrom(val.getClass())) {
        out.println(":");
        ((SimpleRecord)val).prettyPrint(out, depth+1);
        continue;
      } else {
        out.print(" = ");
        out.print(String.valueOf(val));
      }

      out.println();
    }
  }
  
	private String julianToGregorian(double injulian) {
		int JGREG = 15 + 31 * (10 + 12 * 1582);
		int jalpha, ja, jb, jc, jd, je, year, month, day;
		double julian = injulian + 0.5 / 86400.0;
		ja = (int) julian;
		if (ja >= JGREG) {
			jalpha = (int) (((ja - 1867216) - 0.25) / 36524.25);
			ja = ja + 1 + jalpha - jalpha / 4;
		}

		jb = ja + 1524;
		jc = (int) (6680.0 + ((jb - 2439870) - 122.1) / 365.25);
		jd = 365 * jc + jc / 4;
		je = (int) ((jb - jd) / 30.6001);
		day = jb - jd - (int) (30.6001 * je);
		month = je - 1;
		if (month > 12)
			month = month - 12;
		year = jc - 4715;
		if (month > 2)
			year--;
		if (year <= 0)
			year--;

		return String.format("%d-%d-%d", year, month, day);

	}
	
	private String daysInNanosToTime(long nanosTime) {
		long nanos_to_ms = nanosTime / 1000000;

		long hours_from_ms = nanos_to_ms / (60 * 60 * 1000);

		long minutes_from_ms = (nanos_to_ms % (60 * 60 * 1000)) / (60 * 1000);

		long seconds_from_ms = ((nanos_to_ms % (60 * 60 * 1000)) % (60 * 1000)) / 1000;

		long millis_from_ms = nanos_to_ms % 1000;
		
		
		return String.format("%d:%d:%d.%d", hours_from_ms,
				minutes_from_ms, seconds_from_ms, millis_from_ms);
	}


  public static final class NameValue {
    private final String name;
    private final Object value;

    public NameValue(String name, Object value) {
      this.name = name;
      this.value = value;
    }

    public String toString() {
      return name + ": " + value;
    }

    public String getName() {
      return name;
    }

    public Object getValue() {
      return value;
    }
  }
}

