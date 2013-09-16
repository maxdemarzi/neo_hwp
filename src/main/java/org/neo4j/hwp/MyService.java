package org.neo4j.hwp;

import org.codehaus.jackson.map.ObjectMapper;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.kernel.Uniqueness;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.kernel.Traversal;


import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.*;

@Path("/service")
public class MyService {
    private static final RelationshipType PRIOR = DynamicRelationshipType.withName("PRIOR");
    ObjectMapper objectMapper = new ObjectMapper();

    @GET
    @Path("/hwp/{name}")
    public Response getHeaviestWeightedPath(@PathParam("name") String name, @Context GraphDatabaseService db) throws IOException {
        Integer heaviestWeight = 0;
        //org.neo4j.graphdb.Path heaviestPath = null;
        List<Node> heaviestPath = new ArrayList<Node>();
        Map<String, Object> heaviestMap = new HashMap<String, Object>();

        IndexHits<Node> jobNodes = db.index().forNodes("Jobs").get("name", name);
        Node jobNode = jobNodes.getSingle();

        Traverser traverser = Traversal.description()
                .depthFirst()
                .relationships(PRIOR, Direction.OUTGOING)
                .uniqueness(Uniqueness.NONE)
                .evaluator(Evaluators.all())
                .traverse(jobNode);


        for ( org.neo4j.graphdb.Path p : traverser )
        {
            int tempweight = 0;
            Iterator nodes = p.nodes().iterator();
            do {
                Node currentNode = (Node) nodes.next();
                tempweight += (Integer)currentNode.getProperty("weight");
            } while (nodes.hasNext());

            if(tempweight > heaviestWeight) {
                heaviestPath.clear();
                nodes = p.nodes().iterator();
                do {
                    Node currentNode = (Node) nodes.next();
                    heaviestPath.add(currentNode);
                } while (nodes.hasNext());

                heaviestWeight = tempweight;
            }

        }

        heaviestMap.put("weight", heaviestWeight);
        List<Object> nodes = new ArrayList<Object>();
        for ( Node node : heaviestPath )
        {
            Map<String, Object> nodeMap = new HashMap<String, Object>();
            nodeMap.put("id", node.getId());
            nodeMap.put("name", node.getProperty("name"));
            nodeMap.put("weight", node.getProperty("weight"));
            nodes.add(nodeMap);
        }
        heaviestMap.put("nodes", nodes);


        //return String.valueOf(heaviestWeight);
        return Response.ok().entity(objectMapper.writeValueAsString(heaviestMap)).build();
    }
}