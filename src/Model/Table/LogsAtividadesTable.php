<?php
declare(strict_types=1);

namespace App\Model\Table;

use Cake\ORM\Query;
use Cake\ORM\RulesChecker;
use Cake\ORM\Table;
use Cake\Validation\Validator;

/**
 * LogsAtividades Model
 *
 * @method \App\Model\Entity\LogsAtividade newEmptyEntity()
 * @method \App\Model\Entity\LogsAtividade newEntity(array $data, array $options = [])
 * @method \App\Model\Entity\LogsAtividade[] newEntities(array $data, array $options = [])
 * @method \App\Model\Entity\LogsAtividade get($primaryKey, $options = [])
 * @method \App\Model\Entity\LogsAtividade findOrCreate($search, ?callable $callback = null, $options = [])
 * @method \App\Model\Entity\LogsAtividade patchEntity(\Cake\Datasource\EntityInterface $entity, array $data, array $options = [])
 * @method \App\Model\Entity\LogsAtividade[] patchEntities(iterable $entities, array $data, array $options = [])
 * @method \App\Model\Entity\LogsAtividade|false save(\Cake\Datasource\EntityInterface $entity, $options = [])
 * @method \App\Model\Entity\LogsAtividade saveOrFail(\Cake\Datasource\EntityInterface $entity, $options = [])
 * @method \App\Model\Entity\LogsAtividade[]|\Cake\Datasource\ResultSetInterface|false saveMany(iterable $entities, $options = [])
 * @method \App\Model\Entity\LogsAtividade[]|\Cake\Datasource\ResultSetInterface saveManyOrFail(iterable $entities, $options = [])
 * @method \App\Model\Entity\LogsAtividade[]|\Cake\Datasource\ResultSetInterface|false deleteMany(iterable $entities, $options = [])
 * @method \App\Model\Entity\LogsAtividade[]|\Cake\Datasource\ResultSetInterface deleteManyOrFail(iterable $entities, $options = [])
 */
class LogsAtividadesTable extends Table
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

        $this->setTable('logs_atividades');
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
            ->scalar('action')
            ->maxLength('action', 100)
            ->requirePresence('action', 'create')
            ->notEmptyString('action');

        $validator
            ->scalar('remoteURI')
            ->maxLength('remoteURI', 400)
            ->allowEmptyString('remoteURI');

        $validator
            ->scalar('localURI')
            ->maxLength('localURI', 400)
            ->allowEmptyString('localURI');

        $validator
            ->allowEmptyString('result');

        $validator
            ->boolean('hasErrors')
            ->allowEmptyString('hasErrors');

        $validator
            ->scalar('message')
            ->maxLength('message', 400)
            ->allowEmptyString('message');

        $validator
            ->dateTime('date')
            ->requirePresence('date', 'create')
            ->notEmptyDateTime('date');

        $validator
            ->boolean('needToRedo')
            ->notEmptyString('needToRedo');

        return $validator;
    }
}
