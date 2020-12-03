venv:
	virtualenv -p $(shell which python3) venv
	./venv/bin/pip install -r requirements.txt

lab: venv
	./venv/bin/jupyter lab .

clean:
	rm -rf venv
