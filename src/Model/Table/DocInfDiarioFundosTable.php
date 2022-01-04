<?php
declare(strict_types=1);

namespace App\Model\Table;

use Cake\ORM\Query;
use Cake\ORM\RulesChecker;
use Cake\ORM\Table;
use Cake\Validation\Validator;

/**
 * DocInfDiarioFundos Model
 *
 * @property \App\Model\Table\CnpjFundosTable&\Cake\ORM\Association\BelongsTo $CnpjFundos
 *
 * @method \App\Model\Entity\DocInfDiarioFundo newEmptyEntity()
 * @method \App\Model\Entity\DocInfDiarioFundo newEntity(array $data, array $options = [])
 * @method \App\Model\Entity\DocInfDiarioFundo[] newEntities(array $data, array $options = [])
 * @method \App\Model\Entity\DocInfDiarioFundo get($primaryKey, $options = [])
 * @method \App\Model\Entity\DocInfDiarioFundo findOrCreate($search, ?callable $callback = null, $options = [])
 * @method \App\Model\Entity\DocInfDiarioFundo patchEntity(\Cake\Datasource\EntityInterface $entity, array $data, array $options = [])
 * @method \App\Model\Entity\DocInfDiarioFundo[] patchEntities(iterable $entities, array $data, array $options = [])
 * @method \App\Model\Entity\DocInfDiarioFundo|false save(\Cake\Datasource\EntityInterface $entity, $options = [])
 * @method \App\Model\Entity\DocInfDiarioFundo saveOrFail(\Cake\Datasource\EntityInterface $entity, $options = [])
 * @method \App\Model\Entity\DocInfDiarioFundo[]|\Cake\Datasource\ResultSetInterface|false saveMany(iterable $entities, $options = [])
 * @method \App\Model\Entity\DocInfDiarioFundo[]|\Cake\Datasource\ResultSetInterface saveManyOrFail(iterable $entities, $options = [])
 * @method \App\Model\Entity\DocInfDiarioFundo[]|\Cake\Datasource\ResultSetInterface|false deleteMany(iterable $entities, $options = [])
 * @method \App\Model\Entity\DocInfDiarioFundo[]|\Cake\Datasource\ResultSetInterface deleteManyOrFail(iterable $entities, $options = [])
 */
class DocInfDiarioFundosTable extends Table
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

        $this->setTable('doc_inf_diario_fundos');
        $this->setDisplayField('cnpj_fundo_id');
        $this->setPrimaryKey(['cnpj_fundo_id', 'DT_COMPTC']);

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
            ->date('DT_COMPTC')
            ->allowEmptyDate('DT_COMPTC', null, 'create');

        $validator
            ->decimal('VL_TOTAL')
            ->requirePresence('VL_TOTAL', 'create')
            ->notEmptyString('VL_TOTAL');

        $validator
            ->decimal('VL_QUOTA')
            ->requirePresence('VL_QUOTA', 'create')
            ->notEmptyString('VL_QUOTA');

        $validator
            ->decimal('VL_PATRIM_LIQ')
            ->requirePresence('VL_PATRIM_LIQ', 'create')
            ->notEmptyString('VL_PATRIM_LIQ');

        $validator
            ->decimal('CAPTC_DIA')
            ->requirePresence('CAPTC_DIA', 'create')
            ->notEmptyString('CAPTC_DIA');

        $validator
            ->decimal('RESG_DIA')
            ->requirePresence('RESG_DIA', 'create')
            ->notEmptyString('RESG_DIA');

        $validator
            ->integer('NR_COTST')
            ->requirePresence('NR_COTST', 'create')
            ->notEmptyString('NR_COTST');

        $validator
            ->decimal('rentab_diaria')
            ->allowEmptyString('rentab_diaria');

        $validator
            ->decimal('volat_diaria')
            ->requirePresence('volat_diaria', 'create')
            ->notEmptyString('volat_diaria');

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
