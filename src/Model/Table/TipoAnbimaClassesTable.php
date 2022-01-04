<?php
declare(strict_types=1);

namespace App\Model\Table;

use Cake\ORM\Query;
use Cake\ORM\RulesChecker;
use Cake\ORM\Table;
use Cake\Validation\Validator;

/**
 * TipoAnbimaClasses Model
 *
 * @method \App\Model\Entity\TipoAnbimaClass newEmptyEntity()
 * @method \App\Model\Entity\TipoAnbimaClass newEntity(array $data, array $options = [])
 * @method \App\Model\Entity\TipoAnbimaClass[] newEntities(array $data, array $options = [])
 * @method \App\Model\Entity\TipoAnbimaClass get($primaryKey, $options = [])
 * @method \App\Model\Entity\TipoAnbimaClass findOrCreate($search, ?callable $callback = null, $options = [])
 * @method \App\Model\Entity\TipoAnbimaClass patchEntity(\Cake\Datasource\EntityInterface $entity, array $data, array $options = [])
 * @method \App\Model\Entity\TipoAnbimaClass[] patchEntities(iterable $entities, array $data, array $options = [])
 * @method \App\Model\Entity\TipoAnbimaClass|false save(\Cake\Datasource\EntityInterface $entity, $options = [])
 * @method \App\Model\Entity\TipoAnbimaClass saveOrFail(\Cake\Datasource\EntityInterface $entity, $options = [])
 * @method \App\Model\Entity\TipoAnbimaClass[]|\Cake\Datasource\ResultSetInterface|false saveMany(iterable $entities, $options = [])
 * @method \App\Model\Entity\TipoAnbimaClass[]|\Cake\Datasource\ResultSetInterface saveManyOrFail(iterable $entities, $options = [])
 * @method \App\Model\Entity\TipoAnbimaClass[]|\Cake\Datasource\ResultSetInterface|false deleteMany(iterable $entities, $options = [])
 * @method \App\Model\Entity\TipoAnbimaClass[]|\Cake\Datasource\ResultSetInterface deleteManyOrFail(iterable $entities, $options = [])
 */
class TipoAnbimaClassesTable extends Table
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

        $this->setTable('tipo_anbima_classes');
        $this->setDisplayField('id');
        $this->setPrimaryKey('id');
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
            ->scalar('classe_anbima')
            ->maxLength('classe_anbima', 100)
            ->requirePresence('classe_anbima', 'create')
            ->notEmptyString('classe_anbima')
            ->add('classe_anbima', 'unique', ['rule' => 'validateUnique', 'provider' => 'table']);

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
        $rules->add($rules->isUnique(['classe_anbima']));

        return $rules;
    }
}
