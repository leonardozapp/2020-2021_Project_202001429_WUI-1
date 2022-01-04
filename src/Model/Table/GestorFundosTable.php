<?php
declare(strict_types=1);

namespace App\Model\Table;

use Cake\ORM\Query;
use Cake\ORM\RulesChecker;
use Cake\ORM\Table;
use Cake\Validation\Validator;

/**
 * GestorFundos Model
 *
 * @property \App\Model\Table\CadastroFundosTable&\Cake\ORM\Association\HasMany $CadastroFundos
 *
 * @method \App\Model\Entity\GestorFundo newEmptyEntity()
 * @method \App\Model\Entity\GestorFundo newEntity(array $data, array $options = [])
 * @method \App\Model\Entity\GestorFundo[] newEntities(array $data, array $options = [])
 * @method \App\Model\Entity\GestorFundo get($primaryKey, $options = [])
 * @method \App\Model\Entity\GestorFundo findOrCreate($search, ?callable $callback = null, $options = [])
 * @method \App\Model\Entity\GestorFundo patchEntity(\Cake\Datasource\EntityInterface $entity, array $data, array $options = [])
 * @method \App\Model\Entity\GestorFundo[] patchEntities(iterable $entities, array $data, array $options = [])
 * @method \App\Model\Entity\GestorFundo|false save(\Cake\Datasource\EntityInterface $entity, $options = [])
 * @method \App\Model\Entity\GestorFundo saveOrFail(\Cake\Datasource\EntityInterface $entity, $options = [])
 * @method \App\Model\Entity\GestorFundo[]|\Cake\Datasource\ResultSetInterface|false saveMany(iterable $entities, $options = [])
 * @method \App\Model\Entity\GestorFundo[]|\Cake\Datasource\ResultSetInterface saveManyOrFail(iterable $entities, $options = [])
 * @method \App\Model\Entity\GestorFundo[]|\Cake\Datasource\ResultSetInterface|false deleteMany(iterable $entities, $options = [])
 * @method \App\Model\Entity\GestorFundo[]|\Cake\Datasource\ResultSetInterface deleteManyOrFail(iterable $entities, $options = [])
 */
class GestorFundosTable extends Table
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

        $this->setTable('gestor_fundos');
        $this->setDisplayField('id');
        $this->setPrimaryKey('id');

        $this->hasMany('CadastroFundos', [
            'foreignKey' => 'gestor_fundo_id',
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
            ->scalar('cnpj')
            ->maxLength('cnpj', 20)
            ->requirePresence('cnpj', 'create')
            ->notEmptyString('cnpj')
            ->add('cnpj', 'unique', ['rule' => 'validateUnique', 'provider' => 'table']);

        $validator
            ->scalar('nome')
            ->maxLength('nome', 100)
            ->requirePresence('nome', 'create')
            ->notEmptyString('nome');

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
        $rules->add($rules->isUnique(['cnpj']));

        return $rules;
    }
}
