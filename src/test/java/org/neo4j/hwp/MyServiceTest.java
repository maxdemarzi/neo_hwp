package org.neo4j.hwp;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.index.Index;
import org.neo4j.test.TestGraphDatabaseFactory;

import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.HashMap;
import java.util.*;

import static org.junit.Assert.assertEquals;

public class MyServiceTest {

    private GraphDatabaseService db;
    private MyService service;
    private ObjectMapper objectMapper = new ObjectMapper();
    private static final RelationshipType PRIOR = DynamicRelationshipType.withName("PRIOR");
    

    @Before
    public void setUp() {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        dropRootNode(db);
        populateDb(db);
        service = new MyService();
    }

    private void dropRootNode(GraphDatabaseService db){
        Transaction tx = db.beginTx();
        try
        {
            Node root = db.getNodeById(0);
            root.delete();
            tx.success();
        }
        finally
        {
            tx.finish();
        }

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

    @After
    public void tearDown() throws Exception {
        db.shutdown();

    }

    @Test
    public void shouldGetHeaviestWeightedPathFor() throws IOException {
        Response response = service.getHeaviestWeightedPath("A", db);
        Map<String, Object> heaviestMap = objectMapper.readValue((String) response.getEntity(), HashMap.class);
        assertEquals(8, heaviestMap.get("weight"));
        assertEquals("{\"weight\":8,\"nodes\":[{\"id\":1,\"weight\":1,\"name\":\"A\"},{\"id\":6,\"weight\":4,\"name\":\"AB\"},{\"id\":7,\"weight\":3,\"name\":\"AAA\"}]}", response.getEntity());

        response = service.getHeaviestWeightedPath("B", db);
        heaviestMap = objectMapper.readValue((String) response.getEntity(), HashMap.class);
        assertEquals(5, heaviestMap.get("weight"));

        response = service.getHeaviestWeightedPath("C", db);
        heaviestMap = objectMapper.readValue((String) response.getEntity(), HashMap.class);
        assertEquals(7, heaviestMap.get("weight"));

        response = service.getHeaviestWeightedPath("D", db);
        heaviestMap = objectMapper.readValue((String) response.getEntity(), HashMap.class);
        assertEquals(8, heaviestMap.get("weight"));
    }

    public GraphDatabaseService graphdb() {
        return db;
    }
}
