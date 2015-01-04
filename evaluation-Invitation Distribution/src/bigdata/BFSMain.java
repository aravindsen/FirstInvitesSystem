package bigdata;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.math.BigDecimal;
import java.math.RoundingMode;

public class BFSMain {
	private static int numberOfNodes;
	private static int noOfIndividualInvites = 10;
	private static int initialNoOfInvites;
	private static HashMap<String, ArrayList<String>> adjacencyList;
	private static HashMap<String, BigDecimal> probabilityMap;
	private static HashMap<String, Double> clusterCoefficientsMap;
	
	public static void buildAdjacencyList(String path) throws IOException {
		try(BufferedReader br = new BufferedReader(new FileReader(path))) {
			adjacencyList = new HashMap<String, ArrayList<String>>();
			
			int count = 0;
	        String line = br.readLine();
	        while (line != null) {
	        	String[] parts = line.split(":");
//	        	System.out.println(parts[0]);
	        	adjacencyList.put(parts[0], new ArrayList<String>(Arrays.asList(parts[1].split(" "))));
	        	++count;
	            line = br.readLine();
	        }
	        numberOfNodes = count;
	    }
	}
	
	public static void calculateClusterCoefficients(String path) throws IOException {
		try(BufferedReader br = new BufferedReader(new FileReader(path))) {
			clusterCoefficientsMap = new HashMap<String, Double>();
			
	        String line = br.readLine();
	        while (line != null) {
	        	String[] parts = line.split("\t");
//	        	System.out.println(parts[1]);
	        	int degree = adjacencyList.get(parts[0]).size();
	        	double denom = degree*(degree - 1)/2;
	        	double coeff = Integer.parseInt(parts[1])/denom;
	        	clusterCoefficientsMap.put(parts[0], coeff);
	            line = br.readLine();
	        }
	    }
	}
	
	public static void doBfs(String[] initialNodes) {
		int count = initialNodes.length;
		LinkedList<String> nodes = new LinkedList<String>(Arrays.asList(initialNodes));
		probabilityMap = new HashMap<String, BigDecimal>();
		BigDecimal prob ;//= BigDecimal.ZERO;
		//prob.setScale(100, RoundingMode.HALF_UP);
		
		for(int i = 0; i < count; ++i) {
			probabilityMap.put(initialNodes[i], BigDecimal.ONE);
			//prob = prob.add(BigDecimal.ONE);
		}
		int step = 0;
		//System.out.println("Expected at step"+step+": "+prob);
		
		BigDecimal indInvite = new BigDecimal(noOfIndividualInvites);
		indInvite.setScale(100, RoundingMode.HALF_UP);
		BigDecimal tmp;
		while(nodes.peek() != null) {
			String key = nodes.removeFirst();
			if (adjacencyList.containsKey(key)) {
				BigDecimal degree = new BigDecimal(adjacencyList.get(key).size());
				BigDecimal currentNodeValue = probabilityMap.get(key);
				currentNodeValue.setScale(100, RoundingMode.HALF_UP);
				//System.out.println(currentNodeValue);
				BigDecimal edgeProb = indInvite.divide(degree, 100, RoundingMode.HALF_UP);
				BigDecimal edgeProbability = edgeProb.subtract(BigDecimal.ONE).signum() > 0 ? BigDecimal.ONE : edgeProb;

				for (String neighbor: adjacencyList.get(key)) {
					if (probabilityMap.containsKey(neighbor)) {
						BigDecimal oldValue = probabilityMap.get(neighbor);
						tmp = currentNodeValue.multiply(edgeProbability);
						oldValue = oldValue.add(tmp);
						if (oldValue.subtract(BigDecimal.ONE).signum() > 0)
							oldValue = BigDecimal.ONE;
						probabilityMap.put(neighbor, oldValue);
					}
					else {
						tmp = currentNodeValue.multiply(edgeProbability);
						probabilityMap.put(neighbor, tmp);
						nodes.offer(neighbor);
					}
				}
			}
			else {
				//Rare case where a friends relation is not commutative
				System.out.println(key);
			}
			--count;
			if (count == 0) {
				++step;
				prob = BigDecimal.ZERO;
				prob.setScale(100, RoundingMode.HALF_UP);
				for (BigDecimal v: probabilityMap.values()) {
					prob = prob.add(v);
					prob.setScale(100, RoundingMode.HALF_UP);
				}
				System.out.println("Expected at step "+step+": "+prob);
				count = nodes.size();
			}
		}
//		BigDecimal prob = BigDecimal.ZERO;
//		prob.setScale(100, RoundingMode.HALF_UP);
//		BigDecimal min = BigDecimal.TEN;
//		BigDecimal cnt = BigDecimal.ZERO;
//		
//		for (String key: probabilityMap.keySet()) {
//			prob = prob.add(probabilityMap.get(key));
//			if (probabilityMap.get(key).subtract(min).signum() < 0) min = probabilityMap.get(key); 
//			cnt = cnt.add(BigDecimal.ONE);
//		}
//		System.out.println(step);
//		System.out.println("Expected No.: "+prob+"\n"+"Average Prob: "+prob.divide(cnt, 100, RoundingMode.HALF_UP).toPlainString()+"\n"+"Min Prob: "+min.toPlainString());
	}
	
	private static String[] pickTopKClusterCoefficients() {
		String[] nodes = new String[initialNoOfInvites];
		ArrayList<Entry<String, Double>> l = new ArrayList<Entry<String, Double>>();
		for (Entry<String, Double> entry: clusterCoefficientsMap.entrySet()) {
			l.add(entry);
		}
		Collections.sort(l, new Comparator<Entry<String, Double>>(){
			@Override
			public int compare(Entry<String, Double> o1,
					Entry<String, Double> o2) {
				if (o2.getValue() > o1.getValue()) return 1;
				else if (o2.getValue() < o1.getValue()) return -1;
				else {
					int size1 = adjacencyList.containsKey(o1.getKey())?adjacencyList.get(o1.getKey()).size():0;
					int size2 = adjacencyList.containsKey(o2.getKey())?adjacencyList.get(o2.getKey()).size():0;
					return size2 - size1;
				}
			}
		});
		//System.out.println(l.subList(0, 1000));
		int len = l.size();
		boolean goodToPick;
		
		for (int i = 0, picked = 0; i < len && picked < initialNoOfInvites; ++i) {
			goodToPick = true;
			Entry<String, Double> item = l.get(i);
			for (int j = 0; j < picked; ++j) {
				if (adjacencyList.get(nodes[j]).contains(item.getKey())) {
					goodToPick = false;
					break;
				}
			}
			if (goodToPick) nodes[picked++] = item.getKey();
		}
		return nodes;
	}
	
	private static String[] pickTopKDegree() {
		String[] nodes = new String[initialNoOfInvites];
		HashMap<String, Integer> another = new HashMap<String, Integer>(); 
		for (String k: adjacencyList.keySet()) {
			another.put(k, adjacencyList.get(k).size());
		}
		ArrayList<Entry<String, Integer>> l = new ArrayList<Entry<String, Integer>>();
		for (Entry<String, Integer> entry: another.entrySet()) {
			l.add(entry);
		}
		Collections.sort(l, new Comparator<Entry<String, Integer>>(){
			@Override
			public int compare(Entry<String, Integer> o1,
					Entry<String, Integer> o2) {
				return o2.getValue() - o1.getValue();
			}
		});
		//System.out.println(l.subList(0, 1000));
		int picked = 0;
		while (picked < initialNoOfInvites) {
			nodes[picked++] = l.get(picked).getKey();
		}
		return nodes;
	}
	
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		buildAdjacencyList("./yelp_filtered_graph_foodie.txt");
		calculateClusterCoefficients("./triangles.txt");
		
		initialNoOfInvites = 1000;
		doBfs(pickTopKClusterCoefficients());
		doBfs(pickTopKDegree());
	}
}
