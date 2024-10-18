# Migration Lambda

Migração de Análises entre Regiões; Atualização e Restauração de Análises baseados em templates salvos na S3; Criação e Upload de Templates de Análises.

Caso iniciar via console executar: 

```shell
  cd src
  uvicorn lambda_function:app --reload  
```

## Campos
- **aws_access_key_id** : Chave de Acesso AWS
- **aws_secret_access_key** : Senha de Acesso AWS
- **email** : Email do usuário que deseja fazer esta migração. É imprescindível que o usuário tenha as autorizações necessárias para realizar esta atividade.
- **action** : Ação que se deseja realizar. Podendo ser:
    - **MIGRATION** : Para realizar a migração de uma ou mais análises entre a source_region e a target_region.
      - **Requisítos**: 
        - analysis_id, 
        - target_region.
        - stakeholder
    - **TEMPLATE_CREATION** : Para realizar a criação de um template na target_region.
      - **Requisítos**: 
        - analysis_id, 
        - comment,
    - **TEMPLATE_UPDATE** : Para realizar o update de um template existente na target_region.
      - **Requisítos**: 
        - analysis_id,
        - comment,
    - **ANALYSIS_UPDATE** : Atualiza uma análise baseado em um template.
      - **Requisítos**
        - analysis_id,
        - version
    - **LIST_DELETED_ANALYSIS** : Retorna a lista de análises presente na lixeira do Quicksight [30 dias].
    - **RESTORE_ANALYSIS** : Restaura uma análise que esteja na lixeira do Quicksight.
      - **Requisítos**
        - analysis_id
- **analysis_id** : Id das análises que você deseja alterar.
- **source_region** : Região onde a análise fonte se encontra.
- **target_region** : Região para onde se deseja migrar a análise.
- **version** : Versão do Template. Obrigatório somente durante a ação de **ANALYSIS_UPDATE**.
- **comment** : Utilizado durante a ação de TEMPLATE_CREATION e para definir a descrição do template criado. 
- **stakeholder** : Cliente dono do dashboard. Representado por uma pasta na S3 onde os templates são salvos.
- [Link para o Bucket onde os dados são salvos](https://us-east-1.console.aws.amazon.com/s3/buckets/teste-ml-omotor?region=us-east-1&bucketType=general&prefix=quicksight_templates/&showversions=false)

## Event JSON
```python
{
    'email': '',
    'analysis_id': '',
    'stakeholder': ''
    'action': '', # MIGRATION | TEMPLATE_CREATION | TEMPLATE_UPDATE | ANALYSIS_UPDATE | LIST_DELETED_ANALYSIS | RESTORE_ANALYSIS
    'source_region': '', # us-east-1 | us-west-2
    'target_region': '', # us-east-1 | us-west-2
    'version': , 
    'comment': '',
}
```

## S3 Response JSON
```python
{
    'author': '', # email
    'source_region': '',
    'template_id': '',
    'name': '',
    'version': ,
    'comment': '',
    'date': '' #datetime.datetime.now(),
    'analysis_definition': dict 
}
```
[Documentação do Dicionário de analysis_definition](https://docs.aws.amazon.com/quicksight/latest/APIReference/API_DescribeAnalysisDefinition.html)