package bigdata

import groovy.json.JsonSlurper

import org.apache.commons.collections.IteratorUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.Mapper.Context as MapperContext
import org.apache.hadoop.mapreduce.Reducer.Context as ReducerContext
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat

class YelpUserToGraphMR {
    protected static final JsonSlurper jsonSlurper = new JsonSlurper()
    protected static final Text flag = new Text('$')
	protected static final String[] keywords = ["food", "restaurant", "breakfast", "lunch", "dining", "dessert"]
	
	static class RelevantBusinessAndUserMapper extends Mapper<LongWritable, Text, Text, Text> {
		protected static final Text business_id = new Text()
		protected static final Text user_id = new Text()
		
		@Override
		protected void map(LongWritable offset, Text jsonLine, MapperContext context) {
			if (jsonLine.toString()) {
				def json = jsonSlurper.parseText(jsonLine.toString())
				def business = json?.business_id
				def user = json?.user_id
				boolean found
				if (json?.user_id) {
					//User review json
					//print("elam");
					business_id.set("${business}")
					user_id.set("${user}")
					found = true
				}
				else {
					//Business json
					found = false
					for (cat in json?.categories) {
						for (term in keywords) {
							if (cat.toLowerCase().contains(term)) {
								found = true
								break
							}
						}
						if (found) break
					}
					if (found) {
						//print("holo")
						business_id.set("${business}")
						user_id.set(flag.toString())
					}
				}
				if (found) {
					//print(business_id.toString() + ", " + user_id.toString())
					context.write(business_id, user_id)
				}
			}
		}
	}
	
	static class RelevantBusinessAndUserReducer extends Reducer<Text, Text, Text, Text> {
		protected static final Text user = new Text()
		
		@Override
		protected void reduce(Text business, Iterable<Text> users, ReducerContext context) {
			def groups = users.collectMany{[it.toString()]}.groupBy{ it.toString().equals("\$") ? 'dollar' : 'userlist'}
			if (groups.dollar) {
				groups.userlist.each { u ->
					user.set(u) 
					context.write(user, business)
				}
			}
		}
	}
	
	static class RestaurantReviewerMapper extends Mapper<Text, Text, Text, Text> {
		@Override
		protected void map(Text user, Text business, MapperContext context) {
			context.write(user, business)
		}
	}
	
	static class RestaurantReviewerReducer extends Reducer<Text, Text, Text, IntWritable> {
		protected static final IntWritable howMany = new IntWritable()
		
		@Override
		protected void reduce(Text user, Iterable<Text> businesses, ReducerContext context) {
			int cnt = 0
			for (b in businesses) ++cnt
			howMany.set(cnt)
			context.write(user, howMany)
		}
	}
	
    static class FriendsMapper extends Mapper<LongWritable, Text, Text, Text> {
        protected static final Text vertices = new Text()
        protected static final Text degrees = new Text()
        
        @Override
        protected void map(LongWritable offset, Text jsonString, MapperContext context) {
            if (jsonString.toString()) {
                def jsonArray = jsonString.toString().split(':') //jsonSlurper.parseText(jsonString.toString())
                def user = jsonArray[0]	//json?.user_id
                def friends = jsonArray[1].split(' ') //json?.friends
                
                if (user && friends) {
                    friends.each { friend ->
                        if (user != friend) {
                            if (user < friend) {
                                vertices.set("${user},${friend}")
                                degrees.set("${friends.size()},0")
                            } else {
                                vertices.set("${friend},${user}")
                                degrees.set("0,${friends.size()}")
                            }
                            context.write(vertices, degrees)
                        }
                    }
                }
            }
        }
    }
    
    static class FriendsReducer extends Reducer<Text, Text, Text, Text> {
        protected static final Text degrees = new Text()
        
        @Override
        protected void reduce(Text vertices, Iterable<Text> degreesStr, ReducerContext context) {
            long leftDegree = 0, rightDegree = 0
            degreesStr?.each { degree ->
                long[] degreesArray = degree.toString().split(',')*.toLong()
                if (degreesArray.length == 2) {
                    leftDegree += degreesArray[0]
                    rightDegree += degreesArray[1]
                }
            }
            degrees.set("${leftDegree},${rightDegree}")
            context.write(vertices, degrees)
        }
    }
	
    static class NodeIterMapper1 extends Mapper<Text, Text, Text, Text> {
        protected Text leftVertex = new Text()
        protected Text rightVertex = new Text()
        
        @Override
        protected void map(Text vertices, Text degrees, MapperContext context) {
            String[] nodes=vertices.toString().split(',')
            long[] degree = degrees.toString().split(',')*.toLong()
            leftVertex.set(nodes[0])
            rightVertex.set(nodes[1])
            if (degree[0] < degree[1] || (degree[0] == degree[1] && leftVertex.toString() < rightVertex.toString())) {
                context.write(leftVertex, rightVertex)
            }
        }
    }
    
    static class NodeIterReducer1 extends Reducer<Text, Text, Text, Text> {
        protected Text imaginaryEdge = new Text()
        
        @Override
        protected void reduce(Text v, Iterable<Text> neighbors, ReducerContext context) {
			List<String> nList = neighbors.inject([]) { lst, n -> 
				lst << n.toString()
			}
			nList.sort()
			int len = nList.size()
            for (int i = 0; i < len - 1; i++) {
                for (int j = i + 1; j < len; j++) {
                    imaginaryEdge.set("${nList[i]},${nList[j]}")
                    context.write(imaginaryEdge, v)
                }
            }
        }
    }
    
    static class NodeIterMapper2 extends Mapper<Text, Text, Text, Text> {
        @Override
        protected void map(Text edge, Text unknown, MapperContext context) {
            if (unknown.toString().contains(',')) {
                context.write(edge, flag)
            } else {
                context.write(edge, unknown)
            }
        }
    }
    
    static class NodeIterReducer2 extends Reducer<Text, Text, Text, IntWritable> {
        protected static final IntWritable one = new IntWritable(1)
        protected static final Text vertex = new Text()
        
        @Override
        protected void reduce(Text edge, Iterable<Text> unknowns, ReducerContext context) {
            List<String> unknownList = unknowns.inject([]) { lst, n -> 
				lst << n.toString()
			}
			String flagStr = flag.toString()
			
            if (flagStr in unknownList) {
            	List<String> vertices = unknownList.findAll { it != flagStr}
                vertices.each {
                    vertex.set(it)
                    context.write(vertex, one)
                    // For getting triangle count for each vertex
                    edge.toString().split(',').each {
                        vertex.set(it)
                        context.write(vertex, one)
                    }
                }
            }
        }
    }
    
    static class TringleCounterMapper extends Mapper<Text, Text, Text, IntWritable> {
        @Override
        protected void map(Text vertex, Text cnt, MapperContext context) {
			println vertex.toString()
        	println cnt.toString()
            context.write(vertex, new IntWritable(Integer.parseInt(cnt.toString())))
        }
    }
    
    static class TriangleCounterReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        protected static final IntWritable triangleCount = new IntWritable()
        
        @Override
        protected void reduce(Text vertex, Iterable<IntWritable> counts, ReducerContext context) {
            int total = 0
            counts.each { total += it.get() }
            triangleCount.set(total)
            context.write(vertex, triangleCount)
        }
    }
	
    static main(args) {
        Configuration config = new Configuration()
		
		Job businessJob = new Job(config, "Finding restaurant businesses")
		businessJob.jarByClass = YelpUserToGraphMR.class
		businessJob.mapperClass = RelevantBusinessAndUserMapper.class
		businessJob.reducerClass = RelevantBusinessAndUserReducer.class
		businessJob.inputFormatClass = TextInputFormat.class
		businessJob.outputFormatClass = TextOutputFormat.class
		businessJob.outputKeyClass = Text.class
		businessJob.outputValueClass = Text.class
		
		//businessJob.numReduceTasks =  0;
		
		Job userJob = new Job(config, "Finding reviewers of restaurant businesses")
		userJob.jarByClass = YelpUserToGraphMR.class
		userJob.mapperClass = RestaurantReviewerMapper.class
		userJob.reducerClass = RestaurantReviewerReducer.class
		userJob.inputFormatClass = KeyValueTextInputFormat.class
		userJob.outputFormatClass = TextOutputFormat.class
		userJob.outputKeyClass = Text.class
		userJob.outputValueClass = Text.class
		
        Job yelpJob = new Job(config, "Yelp User Data to Graph Job")
        yelpJob.jarByClass = YelpUserToGraphMR.class
        yelpJob.mapperClass = FriendsMapper.class
        yelpJob.reducerClass = FriendsReducer.class
        yelpJob.inputFormatClass = TextInputFormat.class
        yelpJob.outputFormatClass = TextOutputFormat.class
        yelpJob.outputKeyClass = Text.class
        yelpJob.outputValueClass = Text.class
        
        Job iter1Job = new Job(config, "Node Iterator 1")
        iter1Job.jarByClass = YelpUserToGraphMR.class
        iter1Job.mapperClass = NodeIterMapper1.class
        iter1Job.reducerClass = NodeIterReducer1.class
        iter1Job.inputFormatClass = KeyValueTextInputFormat.class
        iter1Job.outputFormatClass = TextOutputFormat.class
        iter1Job.outputKeyClass = Text.class
        iter1Job.outputValueClass = Text.class
//        iter1Job.numReduceTasks = 0
        
        Job iter2Job = new Job(config, "Node Iterator 2")
        iter2Job.jarByClass = YelpUserToGraphMR.class
        iter2Job.mapperClass = NodeIterMapper2.class
        iter2Job.reducerClass = NodeIterReducer2.class
        iter2Job.inputFormatClass = KeyValueTextInputFormat.class
        iter2Job.outputFormatClass = TextOutputFormat.class
        iter2Job.outputKeyClass = Text.class
        iter2Job.outputValueClass = Text.class
//        iter2Job.numReduceTasks = 0
	
        Job triJob = new Job(config, "Triangle Counting")
        triJob.jarByClass = YelpUserToGraphMR.class
        triJob.mapperClass = TringleCounterMapper.class
        triJob.combinerClass = TriangleCounterReducer.class
        triJob.reducerClass = TriangleCounterReducer.class
        triJob.inputFormatClass = KeyValueTextInputFormat.class
        triJob.outputFormatClass = TextOutputFormat.class
        triJob.outputKeyClass = Text.class
        triJob.outputValueClass = IntWritable.class
        
//        Path jsonFile = new Path(args[0])
//        Path edgeDegreeFile = new Path(args[1])
        
//		FileInputFormat.setInputPaths(businessJob, new Path(args[0]))
//		FileOutputFormat.setOutputPath(businessJob, new Path(args[1]))
//		
//		FileInputFormat.setInputPaths(userJob, new Path(args[1]))
//		FileOutputFormat.setOutputPath(userJob, new Path(args[2]))
		
        FileInputFormat.setInputPaths(yelpJob, new Path(args[0]))
        FileOutputFormat.setOutputPath(yelpJob, new Path(args[1]))
        
        FileInputFormat.setInputPaths(iter1Job, new Path(args[1]))
        FileOutputFormat.setOutputPath(iter1Job, new Path(args[2]))
        
		FileInputFormat.setInputPaths(iter2Job, new Path(args[1]), new Path(args[2]))
		FileOutputFormat.setOutputPath(iter2Job, new Path(args[3]))
        
        FileInputFormat.setInputPaths(triJob, new Path(args[3]))
        FileOutputFormat.setOutputPath(triJob, new Path(args[4]))

        //yelpJob.waitForCompletion(true) && 
		//System.exit(businessJob.waitForCompletion(true) && userJob.waitForCompletion(true) ? 0 : 1);
        System.exit(yelpJob.waitForCompletion(true) && iter1Job.waitForCompletion(true) && iter2Job.waitForCompletion(true) && triJob.waitForCompletion(true) ? 0 : 1)
    }
}
