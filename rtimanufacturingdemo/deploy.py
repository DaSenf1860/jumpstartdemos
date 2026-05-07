from pathlib import Path
from fabric_cicd import FabricWorkspace, publish_all_items # 👈 import the function
from azure.identity import AzureCliCredential

token_credential = AzureCliCredential()
repo_dir = Path(__file__).resolve().parent # ...\fabric_items

workspace = FabricWorkspace(
 workspace_id="52b3f806-d1e3-40c1-9188-7efecbc7e4c0",
 token_credential=token_credential,
 repository_directory=str(repo_dir),
 #environment="PROD", # optional, but required if you use parameter replacement via parameter.yml
 #item_type_in_scope=["Eventhouse", "KQLDatabase"], # optional scope
)

publish_all_items(workspace) # 👈 call the function