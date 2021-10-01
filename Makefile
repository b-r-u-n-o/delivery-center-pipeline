init:
	mkdir ./datalake ./datalake/raw ./datalake/processed ./datalake/curated 

pipenv: init
	pip install pipenv
	pipenv shell
	pipenv sync
	
	




