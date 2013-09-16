package org.neo4j.hwp;

import com.sun.jersey.api.client.Client;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.index.Index;
import org.neo4j.server.NeoServer;
import org.neo4j.server.helpers.ServerBuilder;
import org.neo4j.server.rest.JaxRsResponse;
import org.neo4j.server.rest.RestRequest;

import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class MyServiceFunctionalTest {

    public static final Client CLIENT = Client.create();
    public static final String MOUNT_POINT = "/ext";
    private ObjectMapper objectMapper = new ObjectMapper();

    private static final RelationshipType PRIOR = DynamicRelationshipType.withName("PRIOR");

    @Test
    public void shouldReturnConnectedComponentCount() throws IOException {
        NeoServer server = ServerBuilder.server()
                .withThirdPartyJaxRsPackage("org.neo4j.hwp", MOUNT_POINT)
                .build();
        server.start();
        populateDb(server.getDatabase().getGraph());
        RestRequest restRequest = new RestRequest(server.baseUri().resolve(MOUNT_POINT), CLIENT);
        JaxRsResponse response = restRequest.get("service/hwp/A");

        System.out.println(response.getEntity());
        assertEquals("{\"weight\":8,\"nodes\":[{\"id\":1,\"weight\":1,\"name\":\"A\"},{\"id\":6,\"weight\":4,\"name\":\"AB\"},{\"id\":7,\"weight\":3,\"name\":\"AAA\"}]}", response.getEntity());
        server.stop();

    }

    private void populateDb(GraphDatabaseService db) {
        Transaction tx = db.beginTx();
        try
        {
            Node jobA = createJob(db, "A", 1);
            Node jobB = createJob(db, "B", 1);
            Node jobC = createJob(db, "C", 3);
            Node jobD = createJob(db, "D", 4);
            Node jobAA = createJob(db, "AA", 1);
            Node jobAB = createJob(db, "AB", 4);
            Node jobAAA = createJob(db, "AAA", 3);

            jobA.createRelationshipTo(jobAA, PRIOR);
            jobA.createRelationshipTo(jobAB, PRIOR);
            jobAA.createRelationshipTo(jobAAA, PRIOR);
            jobAB.createRelationshipTo(jobAAA, PRIOR);

            Node jobBA = createJob(db, "BA", 1);
            Node jobBB = createJob(db, "BB", 1);
            Node jobABA = createJob(db, "ABA", 2);
            Node jobABB = createJob(db, "ABB", 3);

            jobB.createRelationshipTo(jobBA, PRIOR);
            jobB.createRelationshipTo(jobBB, PRIOR);
            jobBA.createRelationshipTo(jobABA, PRIOR);
            jobBA.createRelationshipTo(jobABB, PRIOR);
            jobBB.createRelationshipTo(jobABA, PRIOR);
            jobBB.createRelationshipTo(jobABB, PRIOR);

            jobC.createRelationshipTo(jobBA, PRIOR);
            jobD.createRelationshipTo(jobBB, PRIOR);

            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    private Node createJob(GraphDatabaseService db, String name, Integer weight) {
        Index<Node> jobs = db.index().forNodes("Jobs");
        Node node = db.createNode();
        node.setProperty("name", name);
        node.setProperty("weight", weight);
        jobs.add(node, "name", name);
        return node;
    }

}
