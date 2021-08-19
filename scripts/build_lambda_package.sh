cd infrastructure

PACKAGE="package"

if [ -d $PACKAGE ]
then
    echo "O Diretório "$PACKAGE" já existe"
else   
    echo "============================================="
    echo "Criando o diretório "$PACKAGE"..."
    mkdir $PACKAGE
    echo "O diretório "$PACKAGE" foi criado"
    echo "============================================="
fi
    
FILE_REQUIREMENTS=../etl/lambda_requiriments.txt


if [ -f $FILE_REQUIREMENTS ]
then
    echo "============================================="
    echo "Instalando dependências localizadas no "$FILE_REQUIREMENTS""
    pip install --target ./package -r $FILE_REQUIREMENTS
    echo "Dependências instaladas com sucesso"
    echo "============================================="
fi


cd $PACKAGE

LAMBDA_FUNCTION=../../etl/lambda_function.py


if [ -f $LAMBDA_FUNCTION ]
then
    echo "============================================="
    echo "Copiando função Handler..."
    cp $LAMBDA_FUNCTION .
    echo "Compactando arquivo lambda_function_payload.zip"
    zip -r9 ../lambda_function_payload.zip .
    echo "Arquivo compactado com sucesso!"
    echo "============================================="
fi

cd ..





