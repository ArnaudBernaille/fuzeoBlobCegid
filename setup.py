# Code setup pour Freeze (creation d'un executable), helas ne fonctionne pas :(

from cx_Freeze import setup, Executable
base = None
#Remplacer "monprogramme.py" par le nom du script qui lance votre programme
executables = [Executable("KARDINAL_GetFichierFromBlob_ImpulsaCegid_abe_2709.py", base=base)]
#Renseignez ici la liste complète des packages utilisés par votre application
packages = ["idna","os","uuid","azure","datetime","pandas","dateutil.parser"]

options = {
    'build_exe': {    
        'packages':packages,
    },
}
#Adaptez les valeurs des variables "name", "version", "description" à votre programme.
setup(
    name = "Mon Programme",
    options = options,
    version = "1.0",
    description = 'Voici mon programme',
    executables = executables
)