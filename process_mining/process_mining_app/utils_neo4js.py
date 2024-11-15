# generate the RG in Neo4j
def createState(tx, state):
    tx.run("CREATE (:State {name: $state})", state=state)
    return None


def createRelationship(tx, state_source, transition_name, transition_label, state_target):
    tx.run('''
    MATCH (x:State),(y:State)
    WHERE x.name = $state_source AND y.name = $state_target
    MERGE (x)-[:Transition {name: $transition_name, label:$transition_label}]->(y)
    ''', state_source=state_source, transition_name=transition_name, transition_label=transition_label, state_target=state_target)
    return None

# Generate REACHABILITY GRAPH di neo4j dari objek model Petrinet


def generate_rg_on_neo4j(net, ts, session, trans_name):
    # session.run("MATCH (N) detach delete (N)")

    for s in ts.states:
        sname = s.name
        createState(session, sname)

    for t in net.transitions:
        tname = t.name
        if t.name not in trans_name:
            tlabel = 'Invisible'
        else:
            tlabel = t.name

        source = t.from_state
        target = t.to_state

        createRelationship(session, source.name, t.name, tlabel, target.name)
    return None

# Cipher untuk membuat petrinet di Neo4J


def createTransition(tx, tname, tlabel):
    tx.run(
        "CREATE (:Transition {type:'master', name: $tname, label: $tlabel })", tname=tname, tlabel=tlabel)
    return None


def createPlace(tx, place):
    tx.run(
        "CREATE (:Place {type:'master', label: 'Invisible', name: $place, token:0, c:0, p:0, m:0, fm:0, inv_incoming: false})", place=place)
    return None


def createRelationship_place_to_transition(tx, place_name, transition_name):
    tx.run('''
    MATCH (x:Place), (y:Transition)
    WHERE x.name = $place_name AND y.name = $transition_name
    MERGE (x)-[:Arc {type:'master', name: $place_name+'_'+$transition_name, f:0, c:0}]->(y)
    ''', place_name=place_name, transition_name=transition_name)
    return None


def createRelationship_transition_to_place(tx, transition_name, place_name):
    tx.run('''
    MATCH (x:Transition), (y:Place)
    WHERE x.name = $transition_name AND y.name = $place_name
    MERGE (x)-[:Arc {type:'master', name: $transition_name+'_'+$place_name, f:0, p:0}]->(y)
    ''', place_name=place_name, transition_name=transition_name)
    return None


def setMasterInitialMarking(tx, im_name):
    tx.run('''
    MATCH (im:Place {name:$im_name})
    SET im.type = 'master', im.im = True, im.token = 1, im.p = 1
    ''', im_name=im_name)
    return None


def setMasterFinalMarking(tx, fm_name):
    tx.run('''
    MATCH (x:Place {name:$fm_name})
    SET x.fm = True
    ''', fm_name=fm_name)
    return None

# Generate model petrinet di neo4j dari objek model Petrinet


def start_environtmen(net, session, ts, initial_marking, final_marking):
    session.run("MATCH (N) detach delete (N)")

    generate_rg_on_neo4j(ts)

    for t in net.transitions:
        if t.label == None:
            tlabel = 'Invisible'
        else:
            tlabel = t.label
        tname = t.name
        createTransition(session, tname, tlabel)

    for p in net.places:
        createPlace(session, p.name)

    for arc in net.arcs:
        source = arc.source
        target = arc.target
        source_class = source.__class__.__name__
        target_class = target.__class__.__name__

        if source_class == 'Place':
            place_name = source.name
            transition_name = target.name
            createRelationship_place_to_transition(
                session, place_name, transition_name)
        else:  # transition
            transition_name = source.name
            place_name = target.name
            createRelationship_transition_to_place(
                session, transition_name, place_name)

    im_name = [im for im in initial_marking][0].name
    setMasterInitialMarking(session, im_name)

    fm_name = [fm for fm in final_marking][0].name
    setMasterFinalMarking(session, fm_name)
    return None


# Cypher to create organizational model
# Cipher untuk membuat petrinet di Neo4J

# contoh entity
# entity_name = team, entity

def createEntity(tx, eName):
    tx.run("CREATE (:Entity {eName:$eName })", eName=eName)
    return None

# def createOrgUnit(tx):
#     createEntity(tx, 'orgUnit')
#     return None
# def createTeam(tx):
#     createEntity(tx, 'team')
#     return None
# def createRole(tx):
#     createEntity(tx, 'role')
    return None


def createResource(tx, rName):
    tx.run("CREATE (:Resource {rName: $rName})", rName=rName)
    return None


def createProductTypeVariable(tx, name, prodType):
    tx.run("CREATE (:Variable {type:'master', name:$name, team: $prodType})",
           name=name, prodType=prodType)
    return None


# fungsi relasi
def createRelationship_team_to_ou(tx):
    tx.run('''
    MATCH (x:Team), (y:OrgUnit)
    MERGE (x)-[:isA]->(y)
    ''')
    return None


def createRelationship_role_to_ou(tx):
    tx.run('''
    MATCH (x:Role), (y:OrgUnit)
    MERGE (x)-[:isA]->(y)
    ''')
    return None


def createRelationship_resource_to_Entity(tx, rName, eName):
    tx.run('''
    MATCH (x:Resource {rName:$rName}), (y:Entity {eName:$eName})
    MERGE (x)<-[:ROLE]-(y)
    ''', rName=rName, eName=eName)


def createRelationship_resource_to_Role(tx, rName, eName):
    tx.run('''
    MATCH (x:Resource {rName:$rName}), (y:Role {eName:$eName})
    MERGE (x)<-[:ROLE]-(y)
    ''', rName=rName, eName=eName)
    return None


def createRelationship_resource_to_Team(tx, rName, tName):
    tx.run('''
    MATCH (x:Resource {rName:$rName}), (y:Team {tName:$tName})
    MERGE (x)<-[:TEAM]-(y)
    ''', rName=rName, tName=tName)
    return None


def createRelationship_entity_supervise_entity(tx, eName1, eName2):
    tx.run('''
    MATCH (x:Entity {eName:$eName1}), (y:Entity {eName:$eName2})
    MERGE (x)-[:SUPERVISED_BY]->(y)
    ''', eName1=eName1, eName2=eName2)


def createRelationship_entity_to_root(tx, eName, rootName):
    tx.run('''
    MATCH (x:Entity {eName:$eName}), (y:Entity {eName:$rootName})
    MERGE (x)-[:TO_ROOT]->(y)
    ''', eName=eName, rootName=rootName)


def createRelationship_task_to_entity(tx, label, eName):
    tx.run('''
    MATCH (x:Transition {label:$label}), (y:Entity {eName:$eName})
    MERGE (x)-[:EXECUTED_BY]->(y)
    ''', label=label, eName=eName)


def createRelationship_task_to_role(tx, label, eName):
    tx.run('''
    MATCH (x:Transition {label:$label}), (y:Role {eName:$eName})
    MERGE (x)-[:EXECUTED_BY]->(y)
    ''', label=label, eName=eName)
    return None


def createRelationship_task_to_team(tx, label, tName):
    tx.run('''
    MATCH (x:Transition {label:$label}), (y:Team {tName:$tName})
    MERGE (x)-[:EXECUTED_BY]->(y)
    ''', label=label, tName=tName)
    return None

# write variable


def createRelationship_task_to_variable(tx, label, name):
    tx.run('''
    MATCH (x:Transition {label:$label}), (y:Variable {name:$name})
    MERGE (x)-[:WRITE]->(y)
    ''', label=label, name=name)
    return None

# read variable


def createRelationship_variable_to_task(tx, var_name, label):
    tx.run('''
    MATCH (y:Variable {name:$var_name}),(x:Transition {label:$label})
    MERGE (y)-[:READ]->(x)
    ''', label=label, var_name=var_name)
    return None


def generate_organizational_model(session, teams, roles, originators):
    # Buat model organisasi
    # createOrgUnit(session)

    for team in teams:
        createEntity(session, team)

    for role in roles:
        createEntity(session, role)

    for name in originators:
        createResource(session, name)

    # Relasi entiti ke entiti
    createRelationship_entity_supervise_entity(
        session, 'Engineer', 'Engineer Manager')
    createRelationship_entity_to_root(session, 'Clerk', 'Structural')
    createRelationship_entity_to_root(
        session, 'Financial Administrator', 'Structural')
    createRelationship_entity_to_root(
        session, 'Engineer Manager', 'Structural')
    createRelationship_entity_to_root(session, 'Customer Service team', 'Team')
    createRelationship_entity_to_root(session, 'Mobile Phone team', 'Team')
    createRelationship_entity_to_root(session, 'GPS team', 'Team')

    # Eksekusi resource ke organizational unit
    createRelationship_resource_to_Entity(session, 'John', 'Clerk')
    createRelationship_resource_to_Entity(session, 'Sue', 'Clerk')
    createRelationship_resource_to_Entity(session, 'Clare', 'Clerk')
    createRelationship_resource_to_Entity(session, 'Mike', 'Engineer Manager')
    createRelationship_resource_to_Entity(session, 'Pete', 'Engineer')
    createRelationship_resource_to_Entity(session, 'Fred', 'Engineer')
    createRelationship_resource_to_Entity(session, 'Robert', 'Engineer')
    createRelationship_resource_to_Entity(
        session, 'Jane', 'Financial Administrator')
    createRelationship_resource_to_Entity(
        session, 'Mona', 'Financial Administrator')

    createRelationship_resource_to_Entity(
        session, 'John', 'Customer Service team')
    createRelationship_resource_to_Entity(session, 'Sue', 'Mobile Phone team')
    createRelationship_resource_to_Entity(session, 'Clare', 'GPS team')
    createRelationship_resource_to_Entity(session, 'Mike', 'Mobile Phone team')
    createRelationship_resource_to_Entity(session, 'Pete', 'Mobile Phone team')
    createRelationship_resource_to_Entity(session, 'Fred', 'GPS team')
    createRelationship_resource_to_Entity(session, 'Robert', 'GPS team')
    createRelationship_resource_to_Entity(session, 'Jane', 'Mobile Phone team')
    createRelationship_resource_to_Entity(session, 'Mona', 'GPS team')

    # Eksekusi relationship aktifitas (model proses) ke organizational model

    createRelationship_task_to_entity(
        session, 'Receive an item and repair request', 'Customer Service team')
    createRelationship_task_to_entity(
        session, 'Receive an item and repair request', 'Clerk')

    createRelationship_task_to_entity(
        session, 'Check the warranty', 'Customer Service team')
    createRelationship_task_to_entity(session, 'Check the warranty', 'Clerk')

    createRelationship_task_to_entity(session, 'Check the item', 'Engineer')
    createRelationship_task_to_entity(session, 'Check the item', 'GPS team')
    createRelationship_task_to_entity(
        session, 'Check the item', 'Mobile Phone team')

    createRelationship_task_to_entity(session, 'Notify the customer', 'Clerk')
    createRelationship_task_to_entity(
        session, 'Notify the customer', 'GPS team')
    createRelationship_task_to_entity(
        session, 'Notify the customer', 'Mobile Phone team')

    createRelationship_task_to_entity(session, 'Repair the item', 'Engineer')
    createRelationship_task_to_entity(session, 'Repair the item', 'GPS team')
    createRelationship_task_to_entity(
        session, 'Repair the item', 'Mobile Phone team')

    createRelationship_task_to_entity(
        session, 'Issue payment', 'Financial Administrator')
    createRelationship_task_to_entity(session, 'Issue payment', 'GPS team')
    createRelationship_task_to_entity(
        session, 'Issue payment', 'Mobile Phone team')

    createRelationship_task_to_entity(
        session, 'Send a cancellation letter', 'Clerk')
    createRelationship_task_to_entity(
        session, 'Send a cancellation letter', 'GPS team')
    createRelationship_task_to_entity(
        session, 'Send a cancellation letter', 'Mobile Phone team')

    createRelationship_task_to_entity(session, 'Return the item', 'Clerk')
    createRelationship_task_to_entity(session, 'Return the item', 'GPS team')
    createRelationship_task_to_entity(
        session, 'Return the item', 'Mobile Phone team')

    createProductTypeVariable(session, 'product_type', '')  # team masih kosong
    createRelationship_task_to_variable(
        session, 'Receive an item and repair request', 'product_type')

    # read
    createRelationship_variable_to_task(
        session, 'product_type', 'Check the item')
    createRelationship_variable_to_task(
        session, 'product_type', 'Notify the customer')
    createRelationship_variable_to_task(
        session, 'product_type', 'Send a cancellation letter')
    createRelationship_variable_to_task(
        session, 'product_type', 'Repair the item')
    createRelationship_variable_to_task(
        session, 'product_type', 'Issue payment')
    createRelationship_variable_to_task(
        session, 'product_type', 'Return the item')
    createRelationship_variable_to_task(
        session, 'product_type', 'Check the item')
    createRelationship_variable_to_task(
        session, 'product_type', 'Check the item')
    createRelationship_variable_to_task(
        session, 'product_type', 'Check the item')


# cloning diidentifikasikan dari type di source place
# tiap cloning bisa dibuat dengan p_id baru
# p_id pada master diabaikan saja, krn hanya untuk diduplikasi oleh cloning nya

# https://neo4j.com/labs/apoc/4.2/overview/apoc.refactor/apoc.refactor.cloneSubgraphFromPaths/

def createCloneFromModelRef(session, p_id):
    q_rootNode = '''
        //MATCH path = (root {type:'master', im:True})-[*]->(m {fm:True}) # semua bagian graf yang akan di clone
        //WITH distinct root AS rootA
        //CALL apoc.refactor.cloneNodes([rootA])
        //YIELD input, output
        //WITH rootA, input, output AS rootB
        //SET rootB.type='clone', rootB.p_id = $p_id

        MaTCH (rootA:Place {type:'master', im:True}) // initial marking
        WITH distinct rootA
        CALL apoc.refactor.cloneNodes([rootA])
        YIELD input, output
        WITH rootA, input, output AS rootB
        SET rootB.type='clone', rootB.p_id = $p_id

        WITH rootA, rootB
        MATCH path = (rootA)-[*]->(node)
        WHERE node.type = 'master'
        WITH rootA, rootB, collect(distinct path) as paths
        CALL apoc.refactor.cloneSubgraphFromPaths(paths, {
            standinNodes:[[rootA, rootB]]
        })
        YIELD input, output, error
        WITH collect(DISTINCT output) AS nodes
        UNWIND nodes as node
        SET node.type = 'clone', node.p_id = $p_id

        RETURN node //input, output, error
    '''
    session.run(q_rootNode, p_id=p_id)
