<?php
declare(strict_types=1);

namespace App\Model\Table;

use Cake\ORM\Query;
use Cake\ORM\RulesChecker;
use Cake\ORM\Table;
use Cake\Validation\Validator;

/**
 * DocExtratosFundos Model
 *
 * @property \App\Model\Table\CnpjFundosTable&\Cake\ORM\Association\BelongsTo $CnpjFundos
 * @property \App\Model\Table\TipoAnbimaClassesTable&\Cake\ORM\Association\BelongsTo $TipoAnbimaClasses
 *
 * @method \App\Model\Entity\DocExtratosFundo newEmptyEntity()
 * @method \App\Model\Entity\DocExtratosFundo newEntity(array $data, array $options = [])
 * @method \App\Model\Entity\DocExtratosFundo[] newEntities(array $data, array $options = [])
 * @method \App\Model\Entity\DocExtratosFundo get($primaryKey, $options = [])
 * @method \App\Model\Entity\DocExtratosFundo findOrCreate($search, ?callable $callback = null, $options = [])
 * @method \App\Model\Entity\DocExtratosFundo patchEntity(\Cake\Datasource\EntityInterface $entity, array $data, array $options = [])
 * @method \App\Model\Entity\DocExtratosFundo[] patchEntities(iterable $entities, array $data, array $options = [])
 * @method \App\Model\Entity\DocExtratosFundo|false save(\Cake\Datasource\EntityInterface $entity, $options = [])
 * @method \App\Model\Entity\DocExtratosFundo saveOrFail(\Cake\Datasource\EntityInterface $entity, $options = [])
 * @method \App\Model\Entity\DocExtratosFundo[]|\Cake\Datasource\ResultSetInterface|false saveMany(iterable $entities, $options = [])
 * @method \App\Model\Entity\DocExtratosFundo[]|\Cake\Datasource\ResultSetInterface saveManyOrFail(iterable $entities, $options = [])
 * @method \App\Model\Entity\DocExtratosFundo[]|\Cake\Datasource\ResultSetInterface|false deleteMany(iterable $entities, $options = [])
 * @method \App\Model\Entity\DocExtratosFundo[]|\Cake\Datasource\ResultSetInterface deleteManyOrFail(iterable $entities, $options = [])
 */
class DocExtratosFundosTable extends Table
{
    /**
     * Initialize method
     *
     * @param array $config The configuration for the Table.
     * @return void
     */
    public function initialize(array $config): void
    {
        parent::initialize($config);

        $this->setTable('doc_extratos_fundos');
        $this->setDisplayField('cnpj_fundo_id');
        $this->setPrimaryKey(['cnpj_fundo_id', 'DT_COMPTC']);

        $this->belongsTo('CnpjFundos', [
            'foreignKey' => 'cnpj_fundo_id',
            'joinType' => 'INNER',
        ]);
        $this->belongsTo('TipoAnbimaClasses', [
            'foreignKey' => 'tipo_anbima_classe_id',
            'joinType' => 'INNER',
        ]);
    }

    /**
     * Default validation rules.
     *
     * @param \Cake\Validation\Validator $validator Validator instance.
     * @return \Cake\Validation\Validator
     */
    public function validationDefault(Validator $validator): Validator
    {
        $validator
            ->date('DT_COMPTC')
            ->allowEmptyDate('DT_COMPTC', null, 'create');

        $validator
            ->decimal('APLIC_MIN')
            ->requirePresence('APLIC_MIN', 'create')
            ->notEmptyString('APLIC_MIN');

        $validator
            ->integer('QT_DIA_PAGTO_COTA')
            ->requirePresence('QT_DIA_PAGTO_COTA', 'create')
            ->notEmptyString('QT_DIA_PAGTO_COTA');

        $validator
            ->integer('QT_DIA_PAGTO_RESGATE')
            ->requirePresence('QT_DIA_PAGTO_RESGATE', 'create')
            ->notEmptyString('QT_DIA_PAGTO_RESGATE');

        $validator
            ->integer('PR_COTA_ETF_MAX')
            ->requirePresence('PR_COTA_ETF_MAX', 'create')
            ->notEmptyString('PR_COTA_ETF_MAX');

        return $validator;
    }

    /**
     * Returns a rules checker object that will be used for validating
     * application integrity.
     *
     * @param \Cake\ORM\RulesChecker $rules The rules object to be modified.
     * @return \Cake\ORM\RulesChecker
     */
    public function buildRules(RulesChecker $rules): RulesChecker
    {
        $rules->add($rules->existsIn(['cnpj_fundo_id'], 'CnpjFundos'));
        $rules->add($rules->existsIn(['tipo_anbima_classe_id'], 'TipoAnbimaClasses'));

        return $rules;
    }
}
