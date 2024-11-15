

def getCurrentMarking(p_id, session):
    q_consumeFinalMarking = '''
            MATCH (cm:Place {p_id: $p_id})
            WHERE cm.token > 0
            RETURN collect(cm.name) AS current_marking
         '''
    results = session.run(q_consumeFinalMarking, p_id=p_id)

    for record in results:
        for e in record:
            return record[0]
# current_marking = getCurrentMarking(5)
# current_marking


def getFinalMarking(p_id, session):
    q_finalMarking = '''
            MATCH (p:Place {p_id: $p_id})
            WHERE p.fm > 0
            RETURN collect(p.name) AS finalMarking
         '''
    results = session.run(q_finalMarking, p_id=p_id)

    for record in results:
        for e in record:
            return record[0]
# current_marking = getCurrentMarking(5)
# current_marking
