# process_mining_app/views.py
from django.shortcuts import render, redirect
from .forms import UploadFileForm
from .utils import prepare_event_log, discover_process_model, visualize_process_model, connect_to_neo4j
import os
from django.conf import settings
from pm4py.objects.petri_net.utils import reachability_graph
from pm4py.visualization.transition_system import visualizer as ts_visualizer
import graphviz
from .utils_ import PetriNetToNeo4j, Generate_Organizational_Model


def process_mining_view(request):
    print("PROCESS")
    # return render(request, 'process_mining_app/index.html')
    if request.method == 'POST':
        form = UploadFileForm(request.POST, request.FILES)
        if form.is_valid():
            uploaded_file = request.FILES['file']
            file_path = os.path.join(settings.MEDIA_ROOT, uploaded_file.name)

            # Save the file to the media directory
            with open(file_path, 'wb+') as destination:
                for chunk in uploaded_file.chunks():
                    destination.write(chunk)

            # Process the uploaded file
            event_log = prepare_event_log(file_path)
            # ----------Main Variable #
            net, initial_marking, final_marking = discover_process_model(
                event_log)
            image_path = visualize_process_model(
                net, initial_marking, final_marking)
            # ---------------------------
            print(f"Generated image Path {image_path}")

            # Make a Reachability Graph Modified Version
            # Construct and modify the reachability graph visualization
            # ---------- Main Variable #
            ts = reachability_graph.construct_reachability_graph(
                net, initial_marking)
            # ------------ #
            gviz = ts_visualizer.apply(ts, parameters={
                                       ts_visualizer.Variants.VIEW_BASED.value.Parameters.FORMAT: "png"})
            dot_string = gviz.source

            # Modify the DOT string to remove IDs from labels
            modified_dot_string = []
            for line in dot_string.splitlines():
                if 'label=' in line:
                    parts = line.split('label="')
                    if len(parts) > 1:
                        label_content = parts[1].split('"')[0]
                        if ',' in label_content:
                            task_label = label_content.split(
                                ",")[1].strip().replace("'", "")
                            task_label = task_label.rstrip(")")
                            line = line.replace(label_content, task_label)
                modified_dot_string.append(line)

            # Create a new Digraph from the modified DOT string and save it
            modified_gviz = graphviz.Source(
                '\n'.join(modified_dot_string), format='png')
            modified_image_path = os.path.join(
                settings.MEDIA_ROOT, 'modified_transition_system')
            print("COBA")
            print(modified_image_path)
            # Construct the path for the template to access the modified image
            modified_image_url = settings.MEDIA_URL + 'modified_transition_system.png'
            print(f"Generated Reachability Graph {modified_image_url}")

            # Connect Database
            # ---------Main Variable----------
            session = connect_to_neo4j()
            # -------------------------------
            petrinet_to_neo4j = PetriNetToNeo4j(
                session, net, initial_marking, final_marking)
            petrinet_to_neo4j.start_environtmen()

            print("Petrinet Sucessfull Initialized!!")

            teams = ['Team', 'Mobile Phone team',
                     'GPS team', 'Customer Service team']
            roles = ['Structural', 'Clerk', 'Engineer Manager',
                     'Engineer', 'Financial Administrator']
            originators = ['John', 'Sue', 'Clare', 'Mike',
                           'Pete', 'Fred', 'Robert', 'Jane', 'Mona']

            print("Starting Graph Organtizational")
            graph_organtizational = Generate_Organizational_Model(
                teams, roles, originators, session)
            graph_organtizational.generate_organizational_model()
            print("SUCESSS--------------------")
            context = {
                'image_path': image_path,
                'reachability_path': modified_image_url,
                'form': form,
            }
            return render(request, 'process_mining_app/process_mining_template.html', context)
    else:
        form = UploadFileForm()
    return render(request, 'process_mining_app/process_mining_template.html', {'form': form})
