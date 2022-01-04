<?php
declare(strict_types=1);

namespace App\Model\Table;

use Cake\ORM\Query;
use Cake\ORM\RulesChecker;
use Cake\ORM\Table;
use Cake\Validation\Validator;

/**
 * SituacaoFundos Model
 *
 * @property \App\Model\Table\CnpjFundosTable&\Cake\ORM\Association\BelongsTo $CnpjFundos
 * @property \App\Model\Table\TipoSituacaoFundosTable&\Cake\ORM\Association\BelongsTo $TipoSituacaoFundos
 *
 * @method \App\Model\Entity\SituacaoFundo newEmptyEntity()
 * @method \App\Model\Entity\SituacaoFundo newEntity(array $data, array $options = [])
 * @method \App\Model\Entity\SituacaoFundo[] newEntities(array $data, array $options = [])
 * @method \App\Model\Entity\SituacaoFundo get($primaryKey, $options = [])
 * @method \App\Model\Entity\SituacaoFundo findOrCreate($search, ?callable $callback = null, $options = [])
 * @method \App\Model\Entity\SituacaoFundo patchEntity(\Cake\Datasource\EntityInterface $entity, array $data, array $options = [])
 * @method \App\Model\Entity\SituacaoFundo[] patchEntities(iterable $entities, array $data, array $options = [])
 * @method \App\Model\Entity\SituacaoFundo|false save(\Cake\Datasource\EntityInterface $entity, $options = [])
 * @method \App\Model\Entity\SituacaoFundo saveOrFail(\Cake\Datasource\EntityInterface $entity, $options = [])
 * @method \App\Model\Entity\SituacaoFundo[]|\Cake\Datasource\ResultSetInterface|false saveMany(iterable $entities, $options = [])
 * @method \App\Model\Entity\SituacaoFundo[]|\Cake\Datasource\ResultSetInterface saveManyOrFail(iterable $entities, $options = [])
 * @method \App\Model\Entity\SituacaoFundo[]|\Cake\Datasource\ResultSetInterface|false deleteMany(iterable $entities, $options = [])
 * @method \App\Model\Entity\SituacaoFundo[]|\Cake\Datasource\ResultSetInterface deleteManyOrFail(iterable $entities, $options = [])
 */
class SituacaoFundosTable extends Table
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

        $this->setTable('situacao_fundos');
        $this->setDisplayField('cnpj_fundo_id');
        $this->setPrimaryKey(['cnpj_fundo_id', 'tipo_situacao_fundo_id', 'DT_INI_SIT']);

        $this->belongsTo('CnpjFundos', [
            'foreignKey' => 'cnpj_fundo_id',
            'joinType' => 'INNER',
        ]);
        $this->belongsTo('TipoSituacaoFundos', [
            'foreignKey' => 'tipo_situacao_fundo_id',
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
            ->date('DT_INI_SIT')
            ->allowEmptyDate('DT_INI_SIT', null, 'create');

        $validator
            ->date('DT_REG_CVM')
            ->requirePresence('DT_REG_CVM', 'create')
            ->notEmptyDate('DT_REG_CVM');

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
        $rules->add($rules->existsIn(['tipo_situacao_fundo_id'], 'TipoSituacaoFundos'));

        return $rules;
    }
}
