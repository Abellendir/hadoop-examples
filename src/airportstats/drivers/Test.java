package airportstats.drivers;

import java.util.HashMap;
import java.util.Map;

public class Test {

	public static void main(String[] args) {
		Map<String, Integer> map = new HashMap<>();
		for(int i = 1; i < 100; i++) {
			map.merge("a", i, Integer::sum);
			System.out.println(map.get("a"));
		}
	}

}
