<?php
declare(strict_types=1);

namespace App\Model\Table;

use Cake\ORM\Query;
use Cake\ORM\RulesChecker;
use Cake\ORM\Table;
use Cake\Validation\Validator;

/**
 * RetornoRiscoFundos Model
 *
 * @property \App\Model\Table\CnpjFundosTable&\Cake\ORM\Association\BelongsTo $CnpjFundos
 *
 * @method \App\Model\Entity\RetornoRiscoFundo newEmptyEntity()
 * @method \App\Model\Entity\RetornoRiscoFundo newEntity(array $data, array $options = [])
 * @method \App\Model\Entity\RetornoRiscoFundo[] newEntities(array $data, array $options = [])
 * @method \App\Model\Entity\RetornoRiscoFundo get($primaryKey, $options = [])
 * @method \App\Model\Entity\RetornoRiscoFundo findOrCreate($search, ?callable $callback = null, $options = [])
 * @method \App\Model\Entity\RetornoRiscoFundo patchEntity(\Cake\Datasource\EntityInterface $entity, array $data, array $options = [])
 * @method \App\Model\Entity\RetornoRiscoFundo[] patchEntities(iterable $entities, array $data, array $options = [])
 * @method \App\Model\Entity\RetornoRiscoFundo|false save(\Cake\Datasource\EntityInterface $entity, $options = [])
 * @method \App\Model\Entity\RetornoRiscoFundo saveOrFail(\Cake\Datasource\EntityInterface $entity, $options = [])
 * @method \App\Model\Entity\RetornoRiscoFundo[]|\Cake\Datasource\ResultSetInterface|false saveMany(iterable $entities, $options = [])
 * @method \App\Model\Entity\RetornoRiscoFundo[]|\Cake\Datasource\ResultSetInterface saveManyOrFail(iterable $entities, $options = [])
 * @method \App\Model\Entity\RetornoRiscoFundo[]|\Cake\Datasource\ResultSetInterface|false deleteMany(iterable $entities, $options = [])
 * @method \App\Model\Entity\RetornoRiscoFundo[]|\Cake\Datasource\ResultSetInterface deleteManyOrFail(iterable $entities, $options = [])
 */
class RetornoRiscoFundosTable extends Table
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

        $this->setTable('retorno_risco_fundos');
        $this->setDisplayField('cnpj_fundo_id');
        $this->setPrimaryKey('cnpj_fundo_id');

        $this->belongsTo('CnpjFundos', [
            'foreignKey' => 'cnpj_fundo_id',
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
            ->integer('periodo_meses')
            ->requirePresence('periodo_meses', 'create')
            ->notEmptyString('periodo_meses');

        $validator
            ->date('data_final')
            ->notEmptyDate('data_final');

        $validator
            ->numeric('rentab_media')
            ->requirePresence('rentab_media', 'create')
            ->notEmptyString('rentab_media');

        $validator
            ->numeric('desvio_padrao')
            ->requirePresence('desvio_padrao', 'create')
            ->notEmptyString('desvio_padrao');

        $validator
            ->integer('num_valores')
            ->allowEmptyString('num_valores');

        $validator
            ->numeric('rentab_min')
            ->allowEmptyString('rentab_min');

        $validator
            ->numeric('rentab_max')
            ->allowEmptyString('rentab_max');

        $validator
            ->integer('meses_abaixo_bench')
            ->allowEmptyString('meses_abaixo_bench');

        $validator
            ->integer('meses_acima_bench')
            ->allowEmptyString('meses_acima_bench');

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

        return $rules;
    }
}
