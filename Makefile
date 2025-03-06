dev-install: generate-requirements dev-compile

generate-requirements: setup-pip-tools
	pip-compile --upgrade requirements.in -o requirements.txt

setup-pip-tools:
	python -m pip install pip-tools
	
dev-compile:
	python -m piptools sync requirements.txt