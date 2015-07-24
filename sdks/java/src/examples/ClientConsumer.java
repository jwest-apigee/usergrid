import org.apache.usergrid.java.client.model.UsergridEntity;

/**
 * Created by ApigeeCorporation on 6/23/15.
 */
public class ClientConsumer {
  public static void main(String[] args) {

    // need a singleton client
//    Client
//        .withClientId("")
//        .withApiUrl("")
//        .withDefaultCollection("")
//    .build();

    //easily delete entity by UUID
//    UsergridEntity.UUID("<uuid>").delete();
//    UsergridEntity.UUID("<uuid>").get();
//    UsergridEntity.Type("pets"').UUID("<uuid>").get();
    // UsergridEntity("Pets", "<uuid>").delete()
//    UsergridEntity.Type("pets").Name("<name>").get();

    // this incurs object creation and GC cost
    new UsergridEntity("<uuid>").delete();

    // DAO pattern: entity
//    UsergridEntity e = new UsergridEntity("<uuid>");
//    e.collection("pets");
//    e.changeType("pets");
//    e.setProperty("breed", "terrier");
//    e.setProperty("color", "black");
//    e.setProperty("weight", "black");
//    e.save();

  // DAO pattern: connection
//    UGConnection ugConnection = new UGConnection();
//    ugConnection.source("<uuid>");
//    ugConnection.target("<uuid>");
//    ugConnection.target("<collection>", "<name>");
//    ugConnection.save();
  }
}
