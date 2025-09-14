import itertools
from . import Neo4jFunc as neo4j_func
#  DATA
import pandas as pd

# Create the data from the image
data = {
    "actor": ["John", "Sue", "Clare", "Mike", "Pete", "Fred", "Robert", "Jane", "Mona"],
    "orgStructure": [
        "Clerk",
        "Clerk",
        "Clerk",
        "Engineer Manager",
        "Engineer",
        "Engineer",
        "Engineer",
        "Financial Administrator",
        "Financial Administrator",
    ],
    "orgTeam": [
        "Customer Service Team",
        "Mobile Phone team",
        "GPS team",
        "Mobile Phone team",
        "Mobile Phone team",
        "GPS team",
        "GPS team",
        "Mobile Phone team",
        "GPS team",
    ],
}

df_res_tab = pd.DataFrame(data)
df_res_tab.set_index("actor", inplace = True)
df_res_tab

import pandas as pd

# Create dataframe from the table in the image
data2 = {
    "activity": [
        "Receive an item and repair request",
        "Check the warranty",
        "Check the item",
        "Notify the customer",
        "Send a cancellation letter",
        "Repair the item",
        "Issue payment",
        "Return the item",
        "START",
        "END",
    ],
    "orgStructure": [
        "Clerk",
        "Clerk",
        "Engineer",
        "Clerk",
        "Clerk",
        "Engineer",
        "Financial Administrator",
        "Clerk",
        None,
        None,
    ],
    "orgTeam": [
        "Customer Service team",
        "Customer Service team",
        "variable",
        "variable",
        "variable",
        "variable",
        "variable",
        "variable",
        None,
        None,
    ],
    "orgTeamVariableName": [
        None,
        None,
        "product_type",
        "product_type",
        "product_type",
        "product_type",
        "product_type",
        "product_type",
        None,
        None,
    ],
}

df_activity_entity = pd.DataFrame(data2)
df_activity_entity.set_index("activity", inplace = True)
df_activity_entity

# Create dataframe from the new image table
data3 = {
    "activity": [
        "Receive an item and repair request",
        "Check the warranty",
        "Check the item",
        "Notify the customer",
        "Send a cancellation letter",
        "Repair the item",
        "Issue payment",
        "Return the item",
        "START",
        "END",
    ],
    "product_type": [
        "write",
        None,
        "read",
        "read",
        "read",
        "read",
        "read",
        "read",
        None,
        None,
    ],
}

df_activity_function = pd.DataFrame(data3)
df_activity_function.set_index("activity", inplace = True)
df_activity_function

# 
def getCurrentMarking(p_id,session):
    q_consumeFinalMarking ='''
            MATCH (cm:Place {p_id: $p_id})
            WHERE cm.token > 0
            RETURN collect(cm.name) AS current_marking
         '''
    results = session.run(q_consumeFinalMarking, p_id=p_id)
    print(f'First mark {results}')
    for record in results:
        for e in record:
            return record[0]
# current_marking = getCurrentMarking(5)
# current_marking

def getFinalMarking(p_id,session):
    q_finalMarking ='''
            MATCH (p:Place {p_id: $p_id})
            WHERE p.fm > 0
            RETURN collect(p.name) AS finalMarking
         '''
    results = session.run(q_finalMarking, p_id=p_id)
    print(f'End mark {results}')
    for record in results:
        for e in record:
            return record[0]
# current_marking = getCurrentMarking(5)
# current_marking
def consumeFinalMarking(p_id,session):
    """
    Consume all remaining tokens in the final marking places of this case (p_id).
    This operation signals that the process instance has ended successfully.
    """

    q_consume = '''
        MATCH (p:Place {p_id: $p_id})
        WHERE p.token > 0
        SET p.token = 0
        RETURN collect(p.name) AS consumed_places
    '''
    results = session.run(q_consume, p_id=p_id)

    consumed_places = []
    for record in results:
        consumed_places = record["consumed_places"]

    print(f"Final marking consumed for case {p_id}: {consumed_places}")
    return consumed_places

# GOTR FUNC
def getAllEmptyInputPlaces(p_id, trans,session):
    q_getAllEmptyInputPlaces = '''
    MATCH (ip_mt: Place {p_id: $p_id})-->(e:Transition {label:$trans})
    WHERE ip_mt.token = 0
    RETURN ip_mt.name
    '''
    results = session.run(q_getAllEmptyInputPlaces, p_id=p_id, trans=trans)


    emptyInputPlaces = []
    for record in results:
        for ip_mt in record:
            emptyInputPlaces.append(ip_mt)
    return emptyInputPlaces

def getAllInputPlaces(p_id, trans,session):
    q_getAllInputPlaces = '''
    MATCH (ip: Place {p_id: $p_id})-->(e:Transition {label:$trans})
    RETURN ip.name
    '''
    results = session.run(q_getAllInputPlaces, p_id=p_id, trans=trans)


    InputPlaces = []
    for record in results:
        for ip in record:
            InputPlaces.append(ip)
    return InputPlaces

# current_marking, contoh: 'p_61'
# target marking, contoh ['p_1', 'p_5']
def checkCandidatesTargetStates(states, target_submarking):
    target_names = target_submarking

#     print(target_names)
    candidate_target_states = []
    for state in states: # periksa tiap state yang ada
        for i in range(len(target_names)): # apakah semua nama target ada dalam satu state?
#             print(target_names[i],'-->', state)
            if target_names[i] not in state: # jika ada nama target yang tidak ada dalam state ini maka break
                break
            if i == len(target_names)-1: # kalau semua nama target place ada dalam state ini maka tambahkan state sbg kandidat
                candidate_target_states.append(state)
    return candidate_target_states

def findspf_rg(source_state, target_state,session):
    q_spf_rg = '''
        MATCH p = allshortestpaths((n {name:$source_state})-[:Transition *..100]->(m {name:$target_state}))
        with p, relationships(p) as rs, [t in relationships(p) | t.name] AS t_names
        WHERE ALL(t IN rs WHERE (t.label = 'Invisible'))
        RETURN length(p) as length, t_names
        limit 1
    '''
    results = session.run(q_spf_rg, source_state=source_state, target_state=target_state)

    emptyInputPlaces = []
    length = 0
    t_names = []
    for record in results:
        length = record[0]
        t_names = record[1]
    return length, t_names

def check_invisible_path(current_state, candidate_target_states):
    spf_len = 1000
    spf_state = ''
    spf_trans = []
#     dummy_current_marking = 'source1'
    source_state = current_state

    for state in candidate_target_states:
#         print('source_state:', source_state)
#         print('state:', state)
        l, t_names = findspf_rg(source_state, state)
#         print(l,' ', state, ' ', spf_len)
        if l>0 and l < spf_len:
            spf_len = l
            spf_state = state # update state dengan jarak terpendek
            spf_trans = t_names
    return spf_state, spf_trans # state untuk update token, t_names untuk update p,c, dan frek
# contoh state: p_121p_151, source1, sink1
def extractTokenPlace(state_to_extract, places):
    tokenPlaces = []
    for place in places:
        if place in state_to_extract:
            tokenPlaces.append(place)

    return tokenPlaces

# contoh activate_transitions = ['skip_2', 'tauSplit_8']

def update_attributes(p_id, activate_transitions,session):
    q_update_state = '''
        with $activate_transitions AS ts
        WITH [i in range(0, size(ts)-1) | {t:ts[i]}] AS pairs
        UNWIND pairs as pair

        MATCH (ip {p_id:$p_id})-[r]->(tr:Transition)
        WHERE tr.name = pair.t
        SET r.f = r.f + 1, ip.token = ip.token - 1, ip.c = ip.c + 1, ip.fm = ip.fm - 1 //, r.c = r.c + 1

        WITH tr, pair
        MATCH (tr)-[s]->(op)
        //WHERE tr.name = pair.t
        WITH distinct s AS s, op
        SET  s.f = s.f + 1, op.token = op.token + 1, op.p = op.p + 1, op.fm = op.fm + 1 //, s.p = s.p + 1
    '''
    session.run(q_update_state, p_id=p_id, activate_transitions=activate_transitions)

    return None

def findStateName(states, placeNames):
    target_names = placeNames
#     print('states: ',states)
#     print('placeNames: ',placeNames)
#     print(target_names)
    candidate_states = []
    for state in states:
        for i in range(len(target_names)):
#             print(target_names[i],'-->', state)
            if target_names[i] not in state:
#                 print('break')
                break
            if i == len(target_names)-1:
                candidate_states.append(state)
#     print('candidate_states: ',candidate_states)
    if candidate_states:
        the_state = min(candidate_states, key=len)
    else:
        the_state = None

    return the_state

# return : kalau kosong maka semua place punya inv_task input,
# kalau ada hasil berarti ini tanda ada place yg tdk punya inv_task input
def allHasInvIncoming(p_id, placesName,session):
    q_allHasInvIncoming = '''
        with $placesName AS ps
        WITH [i in range(0, size(ps)-1) | {p:ps[i]}] AS pairs
        UNWIND pairs as pair
        MATCH (p:Place {p_id:$p_id})
        WITH pair, p, collect(p) as places
        WHERE p.name = pair.p and none(place in places WHERE p.inv_incoming = true) // yang false ditampilkan
        RETURN collect(p) AS place_no_inv_task_input
    '''
    results = session.run(q_allHasInvIncoming, p_id=p_id, placesName=placesName)

    place_no_inv_task_input = []
    for record in results:
        place_no_inv_task_input.extend(record[0])
#     print('place_no_inv_task_input: ', place_no_inv_task_input)
    if place_no_inv_task_input:
        return False
    else:
        return True

# source didapat dari current marking
def getShortestInvPath(p_id, source, target,session):
    q_getShortestInvPath = '''
        OPTIONAL match (source{p_id:$p_id, name:$source, type:'clone'}), (target{name:$target}),
        p = allshortestpaths((source)-[*..20]->(target))
        with p, nodes(p) as ns
        WHERE ALL(node IN ns WHERE (exists(node.label) and node.label = 'Invisible'))
        with p, [n in nodes(p) | n.name] AS ns
        return  ns
        limit 1
        '''
    results = session.run(q_getShortestInvPath, p_id=p_id, source=source, target=target)

    shortestInvpaths = None
    for record in results:
        if record[0] is not None:
            shortestInvpaths = record[0]
    return shortestInvpaths # pasti urutannya place source-->transition-->place--> dst -->place target

# Invisible Replay
# pasti berhasil karena dipanggil setelah memeriksa status enable
def invreplay(p_id, t,session): # t is transition to replay
    q_invreplay = '''
        MATCH (ip:Place {p_id:$p_id, type:'clone'})-[r]->(t:Transition {name:$t})
        SET ip.token = ip.token - 1, r.f = r.f + 1 // langsung update f
        WITH ip, t
        OPTIONAL MATCH (u)-[q]->(ip)
        WHERE q.p > 0
        SET q.f = q.f + 1, q.p = q.p -1 // update f

        WITH distinct t, collect(ip.name) as ips
        MATCH (t)-[r]->(op:Place)
        SET op.token = op.token + 1, r.f = r.f + 1, r.p = r.p + 1 // update p
        RETURN ips, collect(op.name) as ops
        '''
    results = session.run(q_invreplay, p_id=p_id, t=t)
    ips_ops = []
    for record in results:
        for data in record:
            ips_ops.append(data)
    return ips_ops

def setBactToCurrentMarking(p_id, currentMarkingNames,session):
    q_setBactToCurrentMarking ='''
        WITH $currentMarkingNames AS names
        MATCH (p:Place {p_id : $p_id})
        SET p.token = 0
        WITH names
        UNWIND names as name
        MATCH (n:Place {p_id:$p_id}) WHERE n.name IN names
        SET n.token = 1


        //WITH [i in range(0, size(names)) | {name:names[i], value:1}] as pairs
        //UNWIND pairs AS pair
        //MATCH (n:Place {p_id:$p_id}) WHERE n.name = pair.name
        //SET n.token = pair.value
        //RETURN n
     '''
    results = session.run(q_setBactToCurrentMarking, p_id=p_id, currentMarkingNames=currentMarkingNames)

# setBactToCurrentMarking(5, current_marking)

def setBactToCurrentEdgeFrek(p_id, CurrentEdgeFrek,session):
    q_setBactToCurrentMarking ='''
        WITH $CurrentEdgeFrek AS names
        MATCH ()-[e:Arc {p_id : $p_id, type:'clone'}]->()
        SET e.f = 0
        WITH names
        UNWIND names as name
        MATCH ()-[n:Arc {p_id:$p_id, type:'clone'}]->() WHERE n.name IN names
        SET n.f = 1


        //WITH [i in range(0, size(names)) | {name:names[i], value:1}] as pairs
        //UNWIND pairs AS pair
        //MATCH (n:Place {p_id:$p_id}) WHERE n.name = pair.name
        //SET n.token = pair.value
        //RETURN n
     '''
    results = session.run(q_setBactToCurrentMarking, p_id=p_id, CurrentEdgeFrek=CurrentEdgeFrek)

# setBactToCurrentMarking(5, current_marking)

def setBactToCurrentEdgeProduced(p_id, CurrentEdgeProduced,session):
    q_setBactToCurrentMarking ='''
        WITH $CurrentEdgeProduced AS names
        MATCH ()-[e:Arc {p_id : $p_id, type:'clone'}]->()
        SET e.p = 0
        WITH names
        UNWIND names as name
        MATCH ()-[n:Arc {p_id:$p_id, type:'clone'}]->() WHERE n.name IN names
        SET n.p = 1
     '''
    results = session.run(q_setBactToCurrentMarking, p_id=p_id, CurrentEdgeProduced=CurrentEdgeProduced)


def updateConsumedPlaces(p_id, consumed_places,session):
    consumed_places = list(itertools.chain(*consumed_places))
    print("[[[[[consumed_places]]]]] = ", consumed_places)
    q_updateConsumedPlaces = '''
        WITH $consumed_places AS places
        OPTIONAL MATCH (p:Place {p_id:$p_id})
        WHERE p.name IN places
        SET p.c = p.c + 1, p.fm = p.fm - 1//, p.token = p.token - 1
    '''
    session.run(q_updateConsumedPlaces, p_id=p_id, consumed_places=consumed_places)
    return None


def updateProducedPlaces(p_id, produced_places,session):
    produced_places = list(itertools.chain(*produced_places))
    print("[[[[[produced_places]]]]] = ", produced_places)
    q_updateProducedPlacess = '''
        WITH $produced_places AS places
        OPTIONAL MATCH (p:Place {p_id:$p_id})
        WHERE p.name IN places
        SET p.p = p.p + 1, p.fm = p.fm + 1//, p.token = p.token + 1
    '''
    session.run(q_updateProducedPlacess, p_id=p_id, produced_places=produced_places)
    return None

def rollback(p_id, currentMarking, currentEdgeFrek, currentEdgeProduced,session):
    setBactToCurrentMarking(p_id, currentMarking,session)
    setBactToCurrentEdgeFrek(p_id,currentEdgeFrek,session)
    setBactToCurrentEdgeProduced(p_id,currentEdgeProduced,session)
    return None

def getCurrentEdgeFrek(p_id, session):
    q_getCurrentEdgeFrek = '''
        MATCH ()-[e:Arc {p_id: $p_id, type:'clone'}]->()
        WHERE e.f = 1
        RETURN collect(e.name) AS CurrentEdgeFrek
    '''
    result = session.run(q_getCurrentEdgeFrek, p_id=p_id)
    record = result.single()
    return record['CurrentEdgeFrek'] if record else []

def getCurrentEdgeProduced(p_id, session):
    q_getCurrentEdgeProduced = '''
        MATCH ()-[e:Arc {p_id: $p_id, type:'clone'}]->()
        WHERE e.p = 1
        RETURN collect(e.name) AS CurrentEdgeProduced
    '''
    result = session.run(q_getCurrentEdgeProduced, p_id=p_id)
    record = result.single()
    return record['CurrentEdgeProduced'] if record else []

def isEnabled(p_id, trans,session):
    """
    Check if a transition is enabled (can be fired).
    A transition is enabled when all its input places have at least one token.
    
    Args:
        p_id: Process ID
        trans: Transition name
    
    Returns:
        bool: True if transition is enabled, False otherwise
    """
    q_isEnabled = '''
        // Find all input places of the transition
        MATCH (p:Place {p_id: $p_id})-[:Arc {type:'clone'}]->(t:Transition {p_id: $p_id, name: $trans})
        WITH collect(p) as inputPlaces
        
        // Check if all input places have at least one token
        WITH inputPlaces, 
             [p IN inputPlaces WHERE p.token > 0] as placesWithTokens
        
        RETURN size(inputPlaces) > 0 AND size(inputPlaces) = size(placesWithTokens) as isEnabled
    '''
    
    result = session.run(q_isEnabled, p_id=p_id, trans=trans)
    record = result.single()
    
    return record['isEnabled'] if record else False


# algoritma untuk menelusuri invisible (task) move

# penelusuran hanya pada satu emptyInputPlace!
# harus berusaha mencapai target,
# jika gagal maka kirim pesan gagal dan iterasi pada main emptyInputPlace keseluruhan gagal shg hrs rollback
def invisibleMoveRevisi(p_id, target, currentMarking,session):
    # save the current marking
#     currentMarking = getCurrentMarking(p_id)
#     currentEdgeFrek = getCurrentEdgeFrek(p_id)
#     currentEdgeProduced = getCurrentEdgeProduced(p_id)
#     print('current marking === ', currentMarking)

    # get shortest path, sort ASC, and with id
    invisiblePaths = []
    placesConcumed = []
    placesProduced = []
    for p_name in currentMarking: # jika ada 3 input place maka ada 3 iterasi
        print('p_id: ', p_id)
        print('p_name: ', p_name)
        print('target: ', target)
        invisiblePath = getShortestInvPath(p_id, p_name, target) # sementara: hanya diambil 1 yang terpendek
        if invisiblePath is not None:
            invisiblePaths.append(invisiblePath)

    print('invisiblePaths= ', invisiblePaths, len(invisiblePaths))
    if invisiblePaths is None:
        print("<<<<<invisiblePath Is None>>>>>")

    # diurutkan
    sorted_invisiblePaths = sorted(invisiblePaths, key = len)

    # start with first id (shortest) to travel until target if possible, if no then go to 2nd id
    for path in sorted_invisiblePaths:
        print('path = ', path)
        target = path[-1] # target place akhir
        places = []
        transitions = []
        place_type = True
        for i in range(len(path)): # buat list masing2 untuk places dan transitions
            if place_type:
                places.append(path[i])
                place_type = False
            else:
                transitions.append(path[i])
                place_type = True

        for i in range(len(places)): # aktivasi setiap place step by step sampai habis
            if places[i] == target:
                print("====>>>>> target tercapai !!!!!! marking === ", getCurrentMarking(p_id))
                print('placesConcumed: ',placesConcumed)
                print('placesProduced:', placesProduced)

                # Td do: Return harus memastikan semua input place sudah terisi token
                # jadi harus semua iterasi pada sorted_invisiblePaths dilakukan
                # versi ini begitu ada path yang mencapai target langsung selesai
                # perlu diperbaiki agar menlanjutkan ke iterasi berikutnya untuk menjalankan invisible path input place yg lain
                return [True, placesConcumed, placesProduced]  # target tercapai, selesai

            trans = transitions.pop(0) # ambil transition berelasi dg place untuk diperiksa apakah bisa di enable
            print('====> trans = ', trans)
#             current_marking = getCurrentMarking(p_id)
            if isEnabled(p_id, trans,session): # perlu diperiksa krn jk ada input place lain yg tdk ada token maka replay gagal
                print(trans, ' ', 'Is Enabled !!!!!!!!!')

                ips_ops  = invreplay(p_id, trans)  # ips_ops, utk replay tdk perlu info place nya krn semua input place dipakai
                placesConcumed.append(ips_ops[0]) # catat utk dijalankan kalau memang target berhasil replay
                placesProduced.append(ips_ops[1])
            else: # jika gagal di enable maka berhenti disini, tunggu next path sampai habis
                break


    # JIka tidak mencapai return true maka rollback

    # semua path selesai ditelusuri tetapi tidak ada yang berhasil mencapai target sehingga marking perlu di reset (fail to enable)
#     setBactToCurrentMarking(p_id, currentMarking)
#     setBactToCurrentEdgeFrek(p_id,currentEdgeFrek)
#     setBactToCurrentEdgeProduced(p_id,currentEdgeProduced)
    return [False] # return gagal sampai tujuan

# invisible path replay with simulation
def checkInvisiblePathToFillToken(p_id, currentMarkingName, emptyInputPlacesName,session ):
#     print("emptyInputPlaces: ", emptyInputPlacesName)
    consumed_places = []
    produced_places = []
    currentEdgeFrek = getCurrentEdgeFrek(p_id,session) # penyimpanan sementara untuk rollback
    currentEdgeProduced = getCurrentEdgeProduced(p_id,session) # penyimpanan sementara untuk rollback
    progressMarking = currentMarkingName

    # beberapa emptyInputPlaces
    for Ptarget in emptyInputPlacesName:
        result = invisibleMoveRevisi(p_id, Ptarget, progressMarking,session) # true jika berhasil sampai target, false jika gagal
        if result[0]: # True atau False, True jika berhasil sampai target, stop iterasi, update nilai produce consume
            # catat semua state pada place
            consumed_places.extend(result[1])
            produced_places.extend(result[2])
            # tidak break karena perlu cek semua
        else:
            # jika result[0] = False, berarti ada emptyInputPlaces yg empty shg keseluruhan akan gagal meng-enable target
            # maka token di rollback
            rollback(p_id, currentMarkingName, currentEdgeFrek, currentEdgeProduced,session)
            return False
#             break

        progressMarking = getCurrentMarking(p_id,session) # get updated current marking

    # jika semua emptyInputPlaces berhasil dicapai maka state invisible move di update
    print("PLACE DI UPDATE")
    updateConsumedPlaces(p_id, consumed_places,session) # semua
    updateProducedPlaces(p_id, produced_places,session)
    print(getCurrentMarking(p_id,session)) # print marking
    return True # berhasil mencapai target

def invisiblePathIdentificationAndReplay(p_id, currentMarkingName, emptyInputPlacesName, states, places,session):
#     currentMarkingName = getCurrentMarking(p_id)
    print(currentMarkingName)
    source_state = findStateName(states, currentMarkingName) # source_State = string of state name
    print(source_state)

    # kalau ada state yang mengandung semua place current marking
    if source_state:
        target_subState = emptyInputPlacesName # list of place name
        print(target_subState)

        # Algoritma penelusuran invisible path

        # target_substate = list of place names --> all these names should be glue together as a state name
        candidate_target_states = checkCandidatesTargetStates(states, target_subState)
#         print('target adalah: ', candidate_target_states)
        # invisible replay without simulation
        spf_target_state, spf_trans = check_invisible_path(source_state, candidate_target_states) # algoritma inti

#         print('spf_target: ', spf_target_state)
#         print('spf_trans: ', spf_trans)

        if spf_target_state: # kalau ada invisible path
            targetMarking = extractTokenPlace(spf_target_state, places)
            update_attributes(p_id, spf_trans,session)
            return True
        else :
            return False

    # kalau tidak ada, maka terpaksa telusuri satu persatu (tidak via reachability graph)
    else:
        # bisa 2 kondisi
        # 1. jika invisible path berhasil mengisi semua missing token
        # 2. invisible path gagal
        result_status = checkInvisiblePathToFillToken(p_id, currentMarkingName, emptyInputPlacesName,session)
        return result_status
    
    # catat semua nama states dari RG --> list
def reachabilityGraphProperties(ts , net ):
    states = []
    for state in ts.states:
        states.append(state.name)
    places = []
    for place in net.places:
        places.append(place.name)
    return states, places

def findAllCandidateStateTargets(states, target_submarking):
    target_names = target_submarking

    candidate_target_states = []
    for state in states: # periksa tiap state yang ada
        for i in range(len(target_names)): # apakah semua nama target ada dalam satu state?
            if target_names[i] not in state: # jika ada nama target yang tidak ada dalam state ini maka break
                break
            if i == len(target_names)-1: # kalau semua nama target place ada dalam state ini maka tambahkan state sbg kandidat
                candidate_target_states.append(state)
    return candidate_target_states

def findspf_fm_rg(source_state, target_state,session):
    q_spf_fm_rg = '''
        MATCH p = allshortestpaths((n {name:$source_state})-[:Transition *..100]->(m {name:$target_state}))
        with p, relationships(p) as rs, [t in relationships(p) | t.name] AS t_names
        RETURN length(p) as length, t_names
        limit 1
    '''
    results = session.run(q_spf_fm_rg, source_state=source_state, target_state=target_state)

    emptyInputPlaces = []
    length = 0
    t_names = []
    for record in results:
        length = record[0]
        t_names = record[1]
    return length, t_names

def findTheShortestPathOfCurrentMarkingToCandidateTarget(source_state, candidate_target_states):
    spf_len = 1000
    spf_state = ''
    spf_trans = []
    source_state = source_state

    print(source_state, " VS ", candidate_target_states)

    for state in candidate_target_states:
        l, t_names = findspf_fm_rg(source_state, state)
        if l>0 and l < spf_len:
            spf_len = l
            spf_state = state # update state dengan jarak terpendek
            spf_trans = t_names
    return spf_state, spf_trans # state untuk update token, t_names untuk update p,c, dan frek

# currentMarkingName adl list nama places di marking
def replayWithInsertToken(p_id, activity,session):
    inputPlaceNames = getAllInputPlaces(p_id, activity,session) # nama transition
#     updateFM = 0

    # yg penting deteksi ada atau tidak path ke target. kalau ada maka FM pindah, kalau tidak maka FM tetap
#     finalMarkingUpdate = cekUpdateFinalMarking(p_id, finalMarkingName, inputPlaceNames)
#     print('finalMarkingUpdate status:', finalMarkingUpdate)
#     # jika tidak ditemukan path ke target, berarti posisi next activity adalah predesesor dari current marking. jadi memang
#     # tidak perlu update final marking
#     if finalMarkingUpdate:
#         updateFM = 1

    q_replay = '''
        OPTIONAL MATCH (ip_mt: Place {p_id: $p_id})-->(e:Transition {label:$activity})
        WHERE ip_mt.token = 0
        SET ip_mt.token = ip_mt.token + 1 , ip_mt.m = ip_mt.m + 1
        WITH collect(ip_mt.name) as ip_mt_names, count(ip_mt) as num_of_missing_token

        MATCH (ip: Place {p_id: $p_id})-[r]->(t:Transition {label:$activity})
        SET ip.token = ip.token - 1 , ip.c = ip.c + 1, r.c = r.c + 1, r.f = r.f + 1 // langsung update f

        WITH distinct t AS t, ip_mt_names, num_of_missing_token, collect(ip) as ips
        MATCH (t)-[r]->(op)
        SET op.token = op.token + 1, op.p = op.p + 1 , r.p = r.p +1, r.f = r.f + 1 // update FM

        RETURN ip_mt_names, num_of_missing_token, ips, t.label
    '''
    results = session.run(q_replay, activity=activity, p_id = p_id)

    replayInfo = {}
#     print('RETURN ip_mt, num_of_missing_token, ip, op, e')
    for record in results:
        replayInfo['ip_mt_names'] = record[0]
        replayInfo['num_of_missing_token'] = record[1]
        replayInfo['ip'] = record[2]
        replayInfo['e'] = record[3]
#     print('>> replay info dalam fungsi replayWithInsertToken', replayInfo)
    return replayInfo

def replayAndMarkFM(p_id, activity,session):
    q_replay = '''
        MATCH (ip: Place {p_id: $p_id})-[r]->(t:Transition {label:$activity})
        SET ip.token = ip.token - 1 , ip.c = ip.c + 1, ip.fm = 0, r.c = r.c + 1, r.f = r.f + 1 // langsung update f

        WITH distinct t, collect(ip) as ips
        MATCH (t:Transition {label:$activity})-[r]->(op {p_id: $p_id})
        SET op.token = op.token + 1, op.p = op.p + 1 , op.fm = 1, r.p = r.p +1, r.f = r.f + 1 // update p dan f

        RETURN ips, t.label
    '''
    results = session.run(q_replay, activity=activity, p_id = p_id)

    replayInfo = {}
#     print('RETURN ip_mt, num_of_missing_token, ip, op, e')
    for record in results:
        replayInfo['ip_mt'] = None
        replayInfo['num_of_missing_token'] = 0
        replayInfo['ip'] = record[0]
#         replayInfo['fm'] = record[1]
        replayInfo['e'] = record[1]
#     print('>>>>> replay info dalamfungsi replayAndMarkingFM', replayInfo)

    return replayInfo # record berisi nama input place dan jumlah token missing nya

def cekRemainToken(p_id,session):
    q_cekRemainToken = '''
    MATCH (ip:Place {type:'clone', p_id: $p_id})
    WHERE ip.token > 0
    RETURN collect(ip.name) as name, collect(ip.token) as token
    '''
    results = session.run(q_cekRemainToken, p_id=p_id)


    recap = {}
    for record in results:
        recap['name'] = record[0]
        recap['token'] = record[1]
    return recap

def findspf_rg(source_state, target_state, session):
    q_spf_rg = '''
        MATCH p = allshortestpaths((n {name:$source_state})-[:Transition *..100]->(m {name:$target_state}))
        with p, relationships(p) as rs, [t in relationships(p) | t.name] AS t_names
        WHERE ALL(t IN rs WHERE (t.label = 'Invisible'))
        RETURN length(p) as length, t_names
        limit 1
    '''
    results = session.run(q_spf_rg, source_state=source_state, target_state=target_state)

    emptyInputPlaces = []
    length = 0
    t_names = []
    for record in results:
        length = record[0]
        t_names = record[1]
    return length, t_names

def writeVariable(p_id, activity, var_name, var_value,session):
    q_writeVariable = '''
    MATCH (a:Transition {p_id:$p_id, label:$activity})-->(v:Variable {type:'clone', name: $var_name})
    SET v.team = $var_value
    '''
    results = session.run(q_writeVariable, p_id=p_id, activity=activity, var_name=var_name, var_value=var_value)
    return None

def readVariable(p_id, activity, var_name,session):
    q_readVariable = '''
    MATCH (a:Transition {p_id:$p_id, label:$activity})<--(v:Variable {type:'clone', name: $var_name})
    RETURN v.team AS team
    '''
    results = session.run(q_readVariable, p_id=p_id, activity=activity, var_name=var_name)

    recap = {}
    for record in results:
        return record[0]
#         recap['team'] = record[0]
#     return recap['team']

def checkStructural(activity, df_activity_entity):
    return df_activity_entity.loc[activity]['orgStructure']

# memeriksa nama team. Jika bernama 'variable' maka cek nama variable nya dari tabel activity_entity kmdn baca valuenya

def checkTeam(p_id, activity, df_activity_entity,session):
    var_name = ''
    orgTeam =  df_activity_entity.loc[activity]['orgTeam']
    if orgTeam == 'variable': # pasti baca
        var_name = df_activity_entity.loc[activity]['orgTeamVariableName'] # get variable name --> GPS team, Mobile team
        orgTeam = readVariable(p_id, activity, var_name,session) # read variable value in neo4j
    return var_name , orgTeam

def checkTeamFunction(activity, var_name):
    return df_activity_function.loc[activity, var_name]

# get: activity, orgStructure, orgTeam, actor
def scanTheComponentForPattern(p_id, activity, df_activity_entity,session):
    orgStructure = checkStructural(activity, df_activity_entity) # example: engineer, clerk --> checkRoleName
    var_name, orgTeam = checkTeam(p_id, activity, df_activity_entity,session) # 'product_type', 'GPS team' --> checkTeamName
    return orgStructure, orgTeam

def checkOrgStructurePattern(activity, orgStructure, actor,session):
    q_checkOrgStructurePattern = '''
    OPTIONAL MATCH (a:Transition {type:'master', label:$activity})-[:EXECUTED_BY]->(e:Entity {eName:$orgStructure})
    WITH a,e
    MATCH path = (a)-[:EXECUTED_BY]->(e)-[*]->(o:Resource {rName:$actor})
    RETURN length(path) as length, path
    '''
    results = session.run(q_checkOrgStructurePattern, activity=activity, orgStructure=orgStructure, actor=actor)

    length = 0
    path = []
    for record in results:
        length = record[0]
        path = record[1]
    if length>0:
        return True
    else:
        return False
    
def checkOrgTeamPattern(activity, orgTeam, actor,session):
    q_checkOrgTeamPattern = '''
    MATCH path = (a:Transition {type:'master', label:$activity})-[:EXECUTED_BY]->()-[*]->(o:Resource {rName:$actor})
    WITH path, nodes(path) as ns
    WHERE any(n in ns WHERE n.eName=$orgTeam)
    RETURN length(path) as length, path
    '''
    results = session.run(q_checkOrgTeamPattern, activity=activity, orgTeam=orgTeam, actor=actor)

    length = 0
    path = []
    for record in results:
        length = record[0]
        path = record[1]
    if length>0:
        return True
    else:
        return False
    
def recapPerFinalCase(p_id,session):
    """
    Recap token-based replay result for a completed process instance (p_id).
    Works directly on Place node attributes in Neo4j.
    """
    query = """
        MATCH (pl:Place {p_id: $p_id})
        RETURN 
            sum(pl.c) AS consumed,
            sum(pl.p) AS produced,
            sum(pl.m) AS missing,
            collect(CASE WHEN pl.m > 0 THEN pl.name END) AS missing_places,
            sum(pl.token) AS remained,
            collect(CASE WHEN pl.token > 0 THEN pl.name END) AS remained_places,
            collect(CASE WHEN pl.fm > 0 THEN pl.name END) AS final_marking_places
    """
    result = session.run(query, p_id=p_id).single()

    return {
        "consumed": result["consumed"] if result["consumed"] else 0,
        "produced": result["produced"] if result["produced"] else 0,
        "missing": result["missing"] if result["missing"] else 0,
        "m_name": [x for x in result["missing_places"] if x is not None],
        "remained": result["remained"] if result["remained"] else 0,
        "r_name": [x for x in result["remained_places"] if x is not None],
        "fm_name": [x for x in result["final_marking_places"] if x is not None]
    }

def recapEnabledTranLeft(p_id,session):
    """
    Return list of enabled transitions left for process instance (p_id).
    A transition is enabled if *all* its input places contain at least 1 token.
    """
    query = """
        MATCH (t:Transition)<-[:INPUT_TO]-(p:Place {p_id:$p_id})
        WITH t, collect(p.token) AS tokens, collect(p.name) AS places
        // Enabled if none of its input places have zero tokens
        WHERE all(x IN tokens WHERE x > 0)
        RETURN collect({transition: t.name, input_places: places}) AS enabled_transitions
    """
    result = session.run(query, p_id=p_id).single()
    
    return result["enabled_transitions"] if result and result["enabled_transitions"] else []

# Algoritma Utama Online Token Based Replay
from datetime import datetime
import time

def tokenBasedReplay(event_streams,trans_name,states,places,session):
    ###############################
    # Inisialisasi variabel global#
    ###############################
    start_case = {}
    finish_case = {}
    activities_coming = {}
    activate_activities = {} # aktifitas yang sudah pernah diaktivasi
    id_list = [] # list p_id yang sudah dibuat
    anomaly_score = {}
    unknownActivities = {}
    print(f"DALAM FUNGSI GOTR {session}")
    # inisialisasi caselength utk mencatat panjang case yg masih perlu dikerjakan
    caseLength = {}
    for event in event_streams:
        if event[0] in caseLength.keys():
            caseLength[event[0]] = caseLength[event[0]] + 1  # id nya
        else:
            caseLength[event[0]] = 1
#     print('caseLength: ',caseLength)


#     # catat semua nama states dari RG --> list
#     states = []
#     for state in ts.states:
#         states.append(state.name)
# #     states
#     places = []
#     for place in net.places:
#         places.append(place.name)
# #     place

    ####################################################
    # Simulasi konsumsi event dari suatu event-streams #
    ####################################################
    start_gotbr= time.perf_counter()
    print("GO-TBR ", f'Start at {datetime.now()}')
    for event in event_streams:

        #simulasi delay antar event
#         time.sleep(random.random()*2)
#         time.sleep(5)
        
        p_id = event[0]
        activity = event[1]
        actor = event[2]
        prodType_varValue = event[3]
        print(event,caseLength[p_id])
        caseLength[p_id] = caseLength[p_id] - 1 # sisa event makin sedikit
        
#         print('>>>> activated stream: ', activity)

        # inisialisasi variabel local pada case id baru
        if p_id not in id_list: # jika id baru maka lakukan inisialisasi pembuatan model proses dsb
            start_case[p_id]= time.perf_counter()
            print("\033[30m case id: ", p_id, "Start") #f'Start at {datetime.now()}')
            id_list.append(p_id)
            neo4j_func.createCloneFromModelRef(p_id,session)
            activities_coming[p_id] = [activity]
            activate_activities[p_id] = []
            unknownActivities[p_id] = []
            anomaly_score[p_id] = 0
        else:
            activities_coming[p_id].append([activity, actor])

        # Filter event yang tidak dikenali
        if activity not in trans_name:
            unknownActivities[p_id].append(activity)
            print('Unknown transition: ', activity, ', From case id: ', p_id, )
            continue

        # Pada lingkungan online,
        # Untuk menandai akhir dari case bisa ditetapkan berdasarkan posisi final marking,
        # dan batas durasi sejak eksekusi case terakhir
        # jika final marking blm tercapai maka dilakukan insert token atau invisible move jika bisa
        # jika case diketahui telah berakhir maka dapat diberikan kebijakan misalnya dumping case tersebut dll
        sink = False #

        # if currentMarkingName == finalMarkingName:
        #     sink = True
        if caseLength[p_id] == 0:
            sink = True
            
        if sink == False : # belum sampai akhir case

            currentMarkingName = getCurrentMarking(p_id,session)
            finalMarkingName = getFinalMarking(p_id,session)
            # 1. cek apakah ada missing token
            emptyInputPlacesName = getAllEmptyInputPlaces(p_id, activity,session) # list of place name
#             print('emptyInputPlacesName:', emptyInputPlacesName)

            # jika ada missing token
            if emptyInputPlacesName:
                # 2. cek apakah ada invisible path
                if allHasInvIncoming(p_id, emptyInputPlacesName,session):
                    #AHN~
                    if invisiblePathIdentificationAndReplay(p_id, currentMarkingName, emptyInputPlacesName, states, places):
#                         print('1 --> invisible path replay berhasil, lanjut dg replay normal')
                        replay_info= replayAndMarkFM(p_id, activity,session) # invisible path berhasil mencapai target marking
                    else: # invisible path gagal mencapai target marking
#                         print('2 --> invisible path replay gagal')
                        replay_info= replayWithInsertToken(p_id, activity,session)
                else: # ada yang tdk terhubung dg invisible task, maka tidak akan ada invisible path
#                     print('3 --> langsung insert missing token')
#                     print(currentMarkingName)
#                     print(act_stream)
                    replay_info = replayWithInsertToken(p_id, activity,session)
            # aman, bisa langsung replay
            else: # tidak ada emptyInputPlace
#                 print('4 --> semua input place terisi token')
                replay_info = replayAndMarkFM(p_id, activity,session)
#                 print("++++==> remained token = ", cekRemainToken('30'))

            # realtime deviation detection and Warning
            if replay_info['num_of_missing_token'] > 0:
                activate_activities[p_id].append([activity, 'MISSING_TOKEN', len(emptyInputPlacesName)]) # replay_info['num_of_missing_token']])
                anomaly_score[p_id] = anomaly_score[p_id] + 1.0
                print("\033[34m >>ALERT! [", p_id,"][Anomaly score: 1.0, accu scores:", anomaly_score[p_id], "] [",replay_info['e'],"]","[Type: missing token]", actor, "\033[30m")
                if anomaly_score[p_id] >= 1:
                    print("\033[91m >>>WARNING! AN INSPECTION NEEDED ON CASE ID:", p_id, "\033[30m")
            else:
                activate_activities[p_id].append([activity, activity, replay_info['num_of_missing_token']])

#             print("Current Marking is: ", getCurrentMarking(p_id))

#             if caseLength[p_id] == 0:
#                 finish_case[p_id]= time.perf_counter()
#                 print("case id: ", p_id, "Finish") #f'Finish at {datetime.now()}', f' in {round(finish_case[p_id]-start_case[p_id], 2)} second(s)')


        # Jika diperlukan: Reporting begitu case dinyatakan selesai
        else: # sink == true
            consumeFinalMarking(p_id,session)
            print("Case ID:", p_id, "IS FINISHED")
            recap = recapPerFinalCase(p_id,session)
            c = recap['consumed']
            p = recap['produced']
            m = recap['missing']
            m_name = recap['m_name']
            r = recap['remained']
            r_name = recap['r_name']
            fm_name = recap['fm_name']
            #t_enabled = recap['t_enabled']
            print("Token Consumed = ", c)
            print("Token Produced = ", p)
            print("Token Missing = ", m, m_name)
            print("Token Remain = ", r, r_name)
            print("Enabled Transition(s) remain = ", recapEnabledTranLeft(p_id,session))
            print("Fitness = ", ((0.5)*(1 - (m/c))) + ((0.5)*(1-(r/p))))
            print("==============================================")


        # 1. jika aktifitas memiliki fungsi write maka lakukan update variable
        teamF = checkTeamFunction(activity, 'product_type') # df_activity_function
        if teamF == 'write':
            status = writeVariable(p_id, activity,'product_type', prodType_varValue,session) # activity harus ada untuk memastikan bisa write pada model
#             if status

        # 2. periksa role statis (structure), dan role dinamis (team)
        orgStructure, orgTeam = scanTheComponentForPattern(p_id, activity, df_activity_entity) # role dinamis adalah variable

        # pattern
        isStructureConform = checkOrgStructurePattern(activity, orgStructure, actor,session)# statis
        isTeamConform = checkOrgTeamPattern(activity, orgTeam, actor,session) # dinamis
        if isStructureConform and isTeamConform:
            activate_activities[p_id][-1].extend(['normal_originator',actor])
        else:
#             print('orgStructure: ', orgStructure)
#             print('isStructureConform: ',isStructureConform)
            wrong_originator = []
            if not isStructureConform:
                wrong_originator.append('wrong_structure')
                anomaly_score[p_id] = anomaly_score[p_id] + 0.8 # bisa jadi ini adalah work_around
                print("\033[34m >>ALERT! [", p_id,"][Anomaly score: 0.8, accu scores:", anomaly_score[p_id], "] [", replay_info['e'],"]","[Type: wrong_structure]", actor, "\033[30m")
                if anomaly_score[p_id] >= 1:
                    print("\033[91m >>>WARNING! AN INSPECTION NEEDED ON CASE ID:", p_id, "\033[30m")
                #             print('isTeamConform: ', isTeamConform)
            if not isTeamConform:
                wrong_originator.append('wrong_team')
                anomaly_score[p_id] = anomaly_score[p_id] + 0.5 # bisa jadi ini adalah work_around
                print("\033[34m >>ALERT! [", p_id,"][Anomaly score: 0.5, accu scores:", anomaly_score[p_id],"] [", replay_info['e'],"]", "[Type: wrong_team]", actor, "\033[30m")
                if anomaly_score[p_id] >= 1:
                    print("\033[91m >>>WARNING! AN INSPECTION NEEDED ON CASE ID:", p_id, "\033[30m")
            activate_activities[p_id][-1].extend([wrong_originator,actor])

#             dumpFinishedProcess(p_id)

    finish_gotbr= time.perf_counter()
    print("GO-TBR ", f'Finish at {datetime.now()}', f' in {round(finish_gotbr-start_gotbr, 2)} second(s)')

    return activate_activities, activities_coming, unknownActivities

def initialize_case_in_db(p_id, session):
    """
    Creates the cloned graph model in Neo4j for a new process instance.
    This replaces the 'if p_id not in id_list' block.
    """
    print(f"Initializing new case in Neo4j: {p_id}")
    neo4j_func.createCloneFromModelRef(p_id, session)
    return {"status": "initialized", "case_id": p_id}

def process_single_event(p_id, event, trans_name, states, places,check,session):
    """
    Performs conformance checking for a single event against the current
    state of the case in the database. This is the core logic from your old loop.
    """
    print("Event masuk")
    activity = event[1]
    actor = event[2]
    prodType_varValue = event[3]

    # 1. Filter unknown activities
    if activity not in trans_name:
        print(f"Unknown transition: {activity} for case {p_id}")
        return {"status": "deviation", "type": "unknown_activity", "activity": activity}

    # 2. Perform token replay logic
    currentMarkingName = getCurrentMarking(p_id, session)
    emptyInputPlacesName = getAllEmptyInputPlaces(p_id, activity, session)

    if emptyInputPlacesName:
        # Logic for handling missing tokens
        if allHasInvIncoming(p_id, emptyInputPlacesName, session):
            if invisiblePathIdentificationAndReplay(p_id, currentMarkingName, emptyInputPlacesName, states, places, session):
                replay_info = replayAndMarkFM(p_id, activity, session)
            else:
                replay_info = replayWithInsertToken(p_id, activity, session)
        else:
            replay_info = replayWithInsertToken(p_id, activity, session)
    else:
        # Logic for normal replay (no missing tokens)
        replay_info = replayAndMarkFM(p_id, activity, session)

    # 3. Analyze replay results for deviations
    deviation_details = {}
    if replay_info.get('num_of_missing_token', 0) > 0:
        deviation_details['type'] = 'missing_token'
        deviation_details['missing_count'] = replay_info['num_of_missing_token']
        deviation_details['activity'] = activity
        print(f"🚨 ALERT! [Case: {p_id}] [Type: missing_token] for activity '{activity}'")
    if(check=='multi'):
        # 4. Perform organizational conformance check
        teamF = checkTeamFunction(activity, 'product_type')
        if teamF == 'write':
            writeVariable(p_id, activity, 'product_type', prodType_varValue, session)
        orgStructure, orgTeam = scanTheComponentForPattern(p_id, activity, df_activity_entity,session)
        isStructureConform = checkOrgStructurePattern(activity, orgStructure, actor, session)
        isTeamConform = checkOrgTeamPattern(activity, orgTeam, actor, session)
        if not isStructureConform or not isTeamConform:
            org_deviations = []
            if not isStructureConform: org_deviations.append('wrong_structure')
            if not isTeamConform: org_deviations.append('wrong_team')
            deviation_details['type'] = 'organizational'
            deviation_details['org_issues'] = org_deviations
            deviation_details['actor'] = actor
            print(f"🚨 ALERT! [Case: {p_id}] [Type: organizational] by '{actor}' for '{activity}'")
    
    if deviation_details:
        return {"status": "deviation", **deviation_details}
    else:
        return {"status": "conforming", "activity": activity}

# ✅ NEW: Function to finalize a case
def finalize_case(p_id, session):
    """
    Performs final cleanup, calculation, and reporting for a finished case.
    """
    consumeFinalMarking(p_id, session)
    recap = recapPerFinalCase(p_id, session)
    
    # Calculate fitness
    consumed = recap.get('consumed', 0)
    produced = recap.get('produced', 0)
    missing = recap.get('missing', 0)
    remained = recap.get('remained', 0)
    
    fitness = 0
    if consumed > 0 and produced > 0:
        fitness = (0.5 * (1 - (missing / consumed))) + (0.5 * (1 - (remained / produced)))
    
    recap['fitness'] = fitness
    print(f"✅ Case FINISHED: {p_id}, Fitness: {fitness:.2f}")
    
    # You can also dump the case from Neo4j here if needed
    # neo4j_func.dumpFinishedProcess(p_id, session)
    
    return recap