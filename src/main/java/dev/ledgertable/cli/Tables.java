package dev.ledgertable.cli;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Prints rows as an aligned text table. */
final class Tables {
    private Tables() {
    }

    static void print(List<Map<String, Object>> rows) {
        if (rows.isEmpty()) {
            System.out.println("(no rows)");
            return;
        }
        var columns = new ArrayList<>(rows.getFirst().keySet());
        int[] widths = new int[columns.size()];
        for (int c = 0; c < columns.size(); c++) {
            widths[c] = columns.get(c).length();
            for (var row : rows) {
                widths[c] = Math.max(widths[c], String.valueOf(row.get(columns.get(c))).length());
            }
        }
        var line = new StringBuilder();
        for (int c = 0; c < columns.size(); c++) {
            line.append(String.format("%-" + widths[c] + "s  ", columns.get(c)));
        }
        System.out.println(line.toString().stripTrailing());
        for (var row : rows) {
            line.setLength(0);
            for (int c = 0; c < columns.size(); c++) {
                line.append(String.format("%-" + widths[c] + "s  ", row.get(columns.get(c))));
            }
            System.out.println(line.toString().stripTrailing());
        }
    }
}
