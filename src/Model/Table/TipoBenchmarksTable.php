<?php
declare(strict_types=1);

namespace App\Model\Table;

use Cake\ORM\Query;
use Cake\ORM\RulesChecker;
use Cake\ORM\Table;
use Cake\Validation\Validator;

/**
 * TipoBenchmarks Model
 *
 * @property \App\Model\Table\IndicadoresFundosTable&\Cake\ORM\Association\HasMany $IndicadoresFundos
 * @property \App\Model\Table\RelBenchmarksClasseFundosTable&\Cake\ORM\Association\HasMany $RelBenchmarksClasseFundos
 *
 * @method \App\Model\Entity\TipoBenchmark newEmptyEntity()
 * @method \App\Model\Entity\TipoBenchmark newEntity(array $data, array $options = [])
 * @method \App\Model\Entity\TipoBenchmark[] newEntities(array $data, array $options = [])
 * @method \App\Model\Entity\TipoBenchmark get($primaryKey, $options = [])
 * @method \App\Model\Entity\TipoBenchmark findOrCreate($search, ?callable $callback = null, $options = [])
 * @method \App\Model\Entity\TipoBenchmark patchEntity(\Cake\Datasource\EntityInterface $entity, array $data, array $options = [])
 * @method \App\Model\Entity\TipoBenchmark[] patchEntities(iterable $entities, array $data, array $options = [])
 * @method \App\Model\Entity\TipoBenchmark|false save(\Cake\Datasource\EntityInterface $entity, $options = [])
 * @method \App\Model\Entity\TipoBenchmark saveOrFail(\Cake\Datasource\EntityInterface $entity, $options = [])
 * @method \App\Model\Entity\TipoBenchmark[]|\Cake\Datasource\ResultSetInterface|false saveMany(iterable $entities, $options = [])
 * @method \App\Model\Entity\TipoBenchmark[]|\Cake\Datasource\ResultSetInterface saveManyOrFail(iterable $entities, $options = [])
 * @method \App\Model\Entity\TipoBenchmark[]|\Cake\Datasource\ResultSetInterface|false deleteMany(iterable $entities, $options = [])
 * @method \App\Model\Entity\TipoBenchmark[]|\Cake\Datasource\ResultSetInterface deleteManyOrFail(iterable $entities, $options = [])
 */
class TipoBenchmarksTable extends Table
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

        $this->setTable('tipo_benchmarks');
        $this->setDisplayField('nome');
        $this->setPrimaryKey('id');

        $this->hasMany('IndicadoresFundos', [
            'foreignKey' => 'tipo_benchmark_id',
        ]);
        $this->hasMany('RelBenchmarksClasseFundos', [
            'foreignKey' => 'tipo_benchmark_id',
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
            ->integer('id')
            ->allowEmptyString('id', null, 'create');

        $validator
            ->scalar('nome')
            ->maxLength('nome', 400)
            ->requirePresence('nome', 'create')
            ->notEmptyString('nome')
            ->add('nome', 'unique', ['rule' => 'validateUnique', 'provider' => 'table']);

        $validator
            ->scalar('sigla')
            ->maxLength('sigla', 8)
            ->requirePresence('sigla', 'create')
            ->notEmptyString('sigla')
            ->add('sigla', 'unique', ['rule' => 'validateUnique', 'provider' => 'table']);

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
        $rules->add($rules->isUnique(['nome']));
        $rules->add($rules->isUnique(['sigla']));

        return $rules;
    }
}
