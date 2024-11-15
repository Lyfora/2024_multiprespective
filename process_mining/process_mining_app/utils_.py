# Generate model petrinet di neo4j dari objek model Petrinet
from pm4py.objects.petri_net.utils import reachability_graph


class PetriNetToNeo4j:
    def __init__(self, session, net, initial_marking, final_marking):
        self.session = session
        self.net = net
        self.initial_marking = initial_marking
        self.final_marking = final_marking
        self.ts = reachability_graph.construct_reachability_graph(
            self.net, self.initial_marking)

    def setMasterInitialMarking(self, im_name):
        self.session.run('''
        MATCH (im:Place {name:$im_name})
        SET im.type = 'master', im.im = True, im.token = 1, im.p = 1
        ''', im_name=im_name)
        return None

    def setMasterFinalMarking(self, fm_name):
        self.session.run('''
        MATCH (x:Place {name:$fm_name})
        SET x.fm = True
        ''', fm_name=fm_name)
        return None

    def createState(self, state):
        self.session.run("CREATE (:State {name: $state})", state=state)
        return None

    def createRelationship(self, state_source, transition_name, transition_label, state_target):
        self.session.run('''
        MATCH (x:State),(y:State)
        WHERE x.name = $state_source AND y.name = $state_target
        MERGE (x)-[:Transition {name: $transition_name, label:$transition_label}]->(y)
        ''', state_source=state_source, transition_name=transition_name, transition_label=transition_label, state_target=state_target)
        return None

    def generate_rg_on_neo4j(self):
        # session.run("MATCH (N) detach delete (N)")
        # Acuan nama transition di model proses
        trans_name = []
        for t in self.net.transitions:
            trans_name.append(t.label)
        trans_name.append('START')
        trans_name.append('END')

        for s in self.ts.states:
            sname = s.name
            self.createState(sname)

        for t in self.ts.transitions:
            tname = t.name
            if t.name not in trans_name:
                tlabel = 'Invisible'
            else:
                tlabel = t.name

            source = t.from_state
            target = t.to_state
            self.createRelationship(source.name, t.name, tlabel, target.name)
        return None

    # Cipher untuk membuat petrinet di Neo4J
    def createTransition(self, tname, tlabel):
        self.session.run(
            "CREATE (:Transition {type:'master', name: $tname, label: $tlabel })", tname=tname, tlabel=tlabel)
        return None

    def createPlace(self, place):
        self.session.run(
            "CREATE (:Place {type:'master', label: 'Invisible', name: $place, token:0, c:0, p:0, m:0, fm:0, inv_incoming: false})", place=place)
        return None

    def createRelationship_place_to_transition(self, place_name, transition_name):
        self.session.run('''
        MATCH (x:Place), (y:Transition)
        WHERE x.name = $place_name AND y.name = $transition_name
        MERGE (x)-[:Arc {type:'master', name: $place_name+'_'+$transition_name, f:0, c:0}]->(y)
        ''', place_name=place_name, transition_name=transition_name)
        return None

    def createRelationship_transition_to_place(self, transition_name, place_name):
        self.session.run('''
        MATCH (x:Transition), (y:Place)
        WHERE x.name = $transition_name AND y.name = $place_name
        MERGE (x)-[:Arc {type:'master', name: $transition_name+'_'+$place_name, f:0, p:0}]->(y)
        ''', place_name=place_name, transition_name=transition_name)
        return None


## Main-Code ############

    def start_environtmen(self):
        self.session.run("MATCH (N) detach delete (N)")

        self.generate_rg_on_neo4j()

        for t in self.net.transitions:
            if t.label == None:
                tlabel = 'Invisible'
            else:
                tlabel = t.label
            tname = t.name
            self.createTransition(tname, tlabel)

        for p in self.net.places:
            self.createPlace(p.name)

        for arc in self.net.arcs:
            source = arc.source
            target = arc.target
            source_class = source.__class__.__name__
            target_class = target.__class__.__name__

            if source_class == 'Place':
                place_name = source.name
                transition_name = target.name
                self.createRelationship_place_to_transition(
                    place_name, transition_name)
            else:  # transition
                transition_name = source.name
                place_name = target.name
                self.createRelationship_transition_to_place(
                    transition_name, place_name)

        im_name = [im for im in self.initial_marking][0].name
        self.setMasterInitialMarking(im_name)

        fm_name = [fm for fm in self.final_marking][0].name
        self.setMasterFinalMarking(fm_name)
        return None


class Generate_Organizational_Model:
    def __init__(self, teams, roles, originators, session) -> None:
        self.teams = teams
        self.roles = roles
        self.originators = originators
        self.session = session

    def createEntity(self, eName):
        self.session.run("CREATE (:Entity {eName:$eName })", eName=eName)
        return None

    def createResource(self, rName):
        self.session.run("CREATE (:Resource {rName: $rName})", rName=rName)
        return None

    def createProductTypeVariable(self, name, prodType):
        self.session.run(
            "CREATE (:Variable {type:'master', name:$name, team: $prodType})", name=name, prodType=prodType)
        return None

    def createRelationship_entity_supervise_entity(self, eName1, eName2):
        self.session.run('''
        MATCH (x:Entity {eName:$eName1}), (y:Entity {eName:$eName2})
        MERGE (x)-[:SUPERVISED_BY]->(y)
        ''', eName1=eName1, eName2=eName2)
        return None

    def createRelationship_entity_to_root(self, eName, rootName):
        self.session.run('''
        MATCH (x:Entity {eName:$eName}), (y:Entity {eName:$rootName})
        MERGE (x)-[:TO_ROOT]->(y)
        ''', eName=eName, rootName=rootName)
        return None

    def createRelationship_task_to_entity(self, label, eName):
        self.session.run('''
        MATCH (x:Transition {label:$label}), (y:Entity {eName:$eName})
        MERGE (x)-[:EXECUTED_BY]->(y)
        ''', label=label, eName=eName)
        return None

    def createRelationship_task_to_role(self, label, eName):
        self.session.run('''
        MATCH (x:Transition {label:$label}), (y:Role {eName:$eName})
        MERGE (x)-[:EXECUTED_BY]->(y)
        ''', label=label, eName=eName)
        return None

    def createRelationship_task_to_team(self, label, tName):
        self.session.run('''
        MATCH (x:Transition {label:$label}), (y:Team {tName:$tName})
        MERGE (x)-[:EXECUTED_BY]->(y)
        ''', label=label, tName=tName)
        return None

    def createRelationship_task_to_variable(self, label, name):
        self.session.run('''
        MATCH (x:Transition {label:$label}), (y:Variable {name:$name})
        MERGE (x)-[:WRITE]->(y)
        ''', label=label, name=name)
        return None

    def createRelationship_variable_to_task(self, var_name, label):
        self.session.run(
            '''MATCH (y:Variable {name:$var_name}),(x:Transition {label:$label}) MERGE (y)-[:READ]->(x)''', label=label, var_name=var_name)
        return None

    def createRelationship_resource_to_Entity(self, rName, eName):
        self.session.run('''
        MATCH (x:Resource {rName:$rName}), (y:Entity {eName:$eName})
        MERGE (x)<-[:ROLE]-(y)
        ''', rName=rName, eName=eName)
        return None

    def generate_organizational_model(self):
        # Buat model organisasi
        # createOrgUnit(session)

        for team in self.teams:
            self.createEntity(team)

        for role in self.roles:
            self.createEntity(role)

        for name in self.originators:
            self.createResource(name)

        # Relasi entiti ke entiti
        self.createRelationship_entity_supervise_entity(
            'Engineer', 'Engineer Manager')
        self.createRelationship_entity_to_root(
            'Clerk', 'Structural')
        self.createRelationship_entity_to_root(
            'Financial Administrator', 'Structural')
        self.createRelationship_entity_to_root(
            'Engineer Manager', 'Structural')
        self.createRelationship_entity_to_root(
            'Customer Service team', 'Team')
        self.createRelationship_entity_to_root(
            'Mobile Phone team', 'Team')
        self.createRelationship_entity_to_root(
            'GPS team', 'Team')

        # Eksekusi resource ke organizational unit
        self.createRelationship_resource_to_Entity(
            'John', 'Clerk')
        self.createRelationship_resource_to_Entity(
            'Sue', 'Clerk')
        self.createRelationship_resource_to_Entity(
            'Clare', 'Clerk')
        self.createRelationship_resource_to_Entity(
            'Mike', 'Engineer Manager')
        self.createRelationship_resource_to_Entity(
            'Pete', 'Engineer')
        self.createRelationship_resource_to_Entity(
            'Fred', 'Engineer')
        self.createRelationship_resource_to_Entity(
            'Robert', 'Engineer')
        self.createRelationship_resource_to_Entity(
            'Jane', 'Financial Administrator')
        self.createRelationship_resource_to_Entity(
            'Mona', 'Financial Administrator')

        self.createRelationship_resource_to_Entity(
            'John', 'Customer Service team')
        self.createRelationship_resource_to_Entity(
            'Sue', 'Mobile Phone team')
        self.createRelationship_resource_to_Entity(
            'Clare', 'GPS team')
        self.createRelationship_resource_to_Entity(
            'Mike', 'Mobile Phone team')
        self.createRelationship_resource_to_Entity(
            'Pete', 'Mobile Phone team')
        self.createRelationship_resource_to_Entity(
            'Fred', 'GPS team')
        self.createRelationship_resource_to_Entity(
            'Robert', 'GPS team')
        self.createRelationship_resource_to_Entity(
            'Jane', 'Mobile Phone team')
        self.createRelationship_resource_to_Entity(
            'Mona', 'GPS team')

        # Eksekusi relationship aktifitas (model proses) ke organizational model

        self.createRelationship_task_to_entity(
            'Receive an item and repair request', 'Customer Service team')
        self.createRelationship_task_to_entity(
            'Receive an item and repair request', 'Clerk')

        self.createRelationship_task_to_entity(
            'Check the warranty', 'Customer Service team')
        self.createRelationship_task_to_entity(
            'Check the warranty', 'Clerk')

        self.createRelationship_task_to_entity(
            'Check the item', 'Engineer')
        self.createRelationship_task_to_entity(
            'Check the item', 'GPS team')
        self.createRelationship_task_to_entity(
            'Check the item', 'Mobile Phone team')

        self.createRelationship_task_to_entity(
            'Notify the customer', 'Clerk')
        self.createRelationship_task_to_entity(
            'Notify the customer', 'GPS team')
        self.createRelationship_task_to_entity(
            'Notify the customer', 'Mobile Phone team')

        self.createRelationship_task_to_entity(
            'Repair the item', 'Engineer')
        self.createRelationship_task_to_entity(
            'Repair the item', 'GPS team')
        self.createRelationship_task_to_entity(
            'Repair the item', 'Mobile Phone team')

        self.createRelationship_task_to_entity(
            'Issue payment', 'Financial Administrator')
        self.createRelationship_task_to_entity(
            'Issue payment', 'GPS team')
        self.createRelationship_task_to_entity(
            'Issue payment', 'Mobile Phone team')

        self.createRelationship_task_to_entity(
            'Send a cancellation letter', 'Clerk')
        self.createRelationship_task_to_entity(
            'Send a cancellation letter', 'GPS team')
        self.createRelationship_task_to_entity(
            'Send a cancellation letter', 'Mobile Phone team')

        self.createRelationship_task_to_entity(
            'Return the item', 'Clerk')
        self.createRelationship_task_to_entity(
            'Return the item', 'GPS team')
        self.createRelationship_task_to_entity(
            'Return the item', 'Mobile Phone team')

        self.createProductTypeVariable(
            'product_type', '')  # team masih kosong
        self.createRelationship_task_to_variable(
            'Receive an item and repair request', 'product_type')

        # read
        self.createRelationship_variable_to_task(
            'product_type', 'Check the item')
        self.createRelationship_variable_to_task(
            'product_type', 'Notify the customer')
        self.createRelationship_variable_to_task(
            'product_type', 'Send a cancellation letter')
        self.createRelationship_variable_to_task(
            'product_type', 'Repair the item')
        self.createRelationship_variable_to_task(
            'product_type', 'Issue payment')
        self.createRelationship_variable_to_task(
            'product_type', 'Return the item')
        self.createRelationship_variable_to_task(
            'product_type', 'Check the item')
        self.createRelationship_variable_to_task(
            'product_type', 'Check the item')
        self.createRelationship_variable_to_task(
            'product_type', 'Check the item')


class GO_TR:
    def __init__(self, session) -> None:
        self.session = session

    def createCloneFromModelRef(self, p_id):
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
        self.session.run(q_rootNode, p_id=p_id)
        return None

    def getCurrentMarking(self, p_id):
        q_consumeFinalMarking = '''
                MATCH (cm:Place {p_id: $p_id})
                WHERE cm.token > 0
                RETURN collect(cm.name) AS current_marking
            '''
        results = self.session.run(q_consumeFinalMarking, p_id=p_id)
        for record in results:
            for e in record:
                return record[0]

    def getFinalMarking(self, p_id):
        q_finalMarking = '''
                MATCH (p:Place {p_id: $p_id})
                WHERE p.fm > 0
                RETURN collect(p.name) AS finalMarking
            '''
        results = self.session.run(q_finalMarking, p_id=p_id)

        for record in results:
            for e in record:
                return record[0]

    def getAllEmptyInputPlaces(self, p_id, trans):
        q_getAllEmptyInputPlaces = '''
        MATCH (ip_mt: Place {p_id: $p_id})-->(e:Transition {label:$trans})
        WHERE ip_mt.token = 0
        RETURN ip_mt.name
        '''
        results = self.session.run(
            q_getAllEmptyInputPlaces, p_id=p_id, trans=trans)

        emptyInputPlaces = []
        for record in results:
            for ip_mt in record:
                emptyInputPlaces.append(ip_mt)
        return emptyInputPlaces

    # return : kalau kosong maka semua place punya inv_task input,
# kalau ada hasil berarti ini tanda ada place yg tdk punya inv_task input
    def allHasInvIncoming(self, p_id, placesName):
        q_allHasInvIncoming = '''
            with $placesName AS ps
            WITH [i in range(0, size(ps)-1) | {p:ps[i]}] AS pairs
            UNWIND pairs as pair
            MATCH (p:Place {p_id:$p_id})
            WITH pair, p, collect(p) as places
            WHERE p.name = pair.p and none(place in places WHERE p.inv_incoming = true) // yang false ditampilkan
            RETURN collect(p) AS place_no_inv_task_input
        '''
        results = self.session.run(
            q_allHasInvIncoming, p_id=p_id, placesName=placesName)

        place_no_inv_task_input = []
        for record in results:
            place_no_inv_task_input.extend(record[0])
    #     print('place_no_inv_task_input: ', place_no_inv_task_input)
        if place_no_inv_task_input:
            return False
        else:
            return True

    def invisiblePathIdentificationAndReplay(p_id, currentMarkingName, emptyInputPlacesName, states, places):
        #     currentMarkingName = getCurrentMarking(p_id)
    print(currentMarkingName)
    # source_State = string of state name
    source_state = findStateName(states, currentMarkingName)
    print(source_state)

    # kalau ada state yang mengandung semua place current marking
    if source_state:
        target_subState = emptyInputPlacesName  # list of place name
        print(target_subState)

        # Algoritma penelusuran invisible path

        # target_substate = list of place names --> all these names should be glue together as a state name
        candidate_target_states = checkCandidatesTargetStates(
            states, target_subState)
#         print('target adalah: ', candidate_target_states)
        # invisible replay without simulation
        spf_target_state, spf_trans = check_invisible_path(
            source_state, candidate_target_states)  # algoritma inti

#         print('spf_target: ', spf_target_state)
#         print('spf_trans: ', spf_trans)

        if spf_target_state:  # kalau ada invisible path
            targetMarking = extractTokenPlace(spf_target_state, places)
            update_attributes(p_id, spf_trans)
            return True
        else:
            return False

    # kalau tidak ada, maka terpaksa telusuri satu persatu (tidak via reachability graph)
    else:
        # bisa 2 kondisi
        # 1. jika invisible path berhasil mengisi semua missing token
        # 2. invisible path gagal
        result_status = checkInvisiblePathToFillToken(
            p_id, currentMarkingName, emptyInputPlacesName)
        return result_status
