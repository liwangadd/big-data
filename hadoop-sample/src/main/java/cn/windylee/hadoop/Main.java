package cn.windylee.hadoop;

import java.util.*;

public class Main {

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        int n = scanner.nextInt();
        int[] lens = new int[n];
        int[] vals = new int[n];
        for (int i = 0; i < n; ++i) lens[i] = scanner.nextInt();
        for (int i = 0; i < n; ++i) vals[i] = scanner.nextInt();
        TreeMap<Integer, List<Integer>> map = new TreeMap<>((o1, o2) -> o2 - o1);
        for (int i = 0; i < n; ++i) {
            if (map.containsKey(lens[i])) map.get(lens[i]).add(i);
            else map.put(lens[i], new ArrayList<>(Arrays.asList(i)));
        }
        int res = 0, total = n;
        while (map.get(map.firstKey()).size() <= total / 2) {
            List<Integer> indexes = map.get(map.firstKey());
            total -= indexes.size();
            for (Integer index : indexes) {
                res += vals[index];
            }
            map.remove(map.firstKey());
        }
        System.out.println(res);
    }

}
