import os
from groq import Groq

# Put your exact Groq API key here
os.environ["GROQ_API_KEY"] = "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"

client = Groq()

print("🟢 Currently Active Groq Models:")
print("-" * 30)

# This fetches the live list of models you have access to
models = client.models.list()
for model in models.data:
    print(model.id)