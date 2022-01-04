<?php
declare(strict_types=1);

namespace App\Model\Table;

use Cake\ORM\Query;
use Cake\ORM\RulesChecker;
use Cake\ORM\Table;
use Cake\Validation\Validator;

/**
 * TipoOperacoesFinanceiras Model
 *
 * @property \App\Model\Table\OperacoesFinanceirasTable&\Cake\ORM\Association\HasMany $OperacoesFinanceiras
 *
 * @method \App\Model\Entity\TipoOperacoesFinanceira newEmptyEntity()
 * @method \App\Model\Entity\TipoOperacoesFinanceira newEntity(array $data, array $options = [])
 * @method \App\Model\Entity\TipoOperacoesFinanceira[] newEntities(array $data, array $options = [])
 * @method \App\Model\Entity\TipoOperacoesFinanceira get($primaryKey, $options = [])
 * @method \App\Model\Entity\TipoOperacoesFinanceira findOrCreate($search, ?callable $callback = null, $options = [])
 * @method \App\Model\Entity\TipoOperacoesFinanceira patchEntity(\Cake\Datasource\EntityInterface $entity, array $data, array $options = [])
 * @method \App\Model\Entity\TipoOperacoesFinanceira[] patchEntities(iterable $entities, array $data, array $options = [])
 * @method \App\Model\Entity\TipoOperacoesFinanceira|false save(\Cake\Datasource\EntityInterface $entity, $options = [])
 * @method \App\Model\Entity\TipoOperacoesFinanceira saveOrFail(\Cake\Datasource\EntityInterface $entity, $options = [])
 * @method \App\Model\Entity\TipoOperacoesFinanceira[]|\Cake\Datasource\ResultSetInterface|false saveMany(iterable $entities, $options = [])
 * @method \App\Model\Entity\TipoOperacoesFinanceira[]|\Cake\Datasource\ResultSetInterface saveManyOrFail(iterable $entities, $options = [])
 * @method \App\Model\Entity\TipoOperacoesFinanceira[]|\Cake\Datasource\ResultSetInterface|false deleteMany(iterable $entities, $options = [])
 * @method \App\Model\Entity\TipoOperacoesFinanceira[]|\Cake\Datasource\ResultSetInterface deleteManyOrFail(iterable $entities, $options = [])
 */
class TipoOperacoesFinanceirasTable extends Table
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

        $this->setTable('tipo_operacoes_financeiras');
        $this->setDisplayField('nome');
        $this->setPrimaryKey('id');

        $this->hasMany('OperacoesFinanceiras', [
            'foreignKey' => 'tipo_operacoes_financeira_id',
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
            ->maxLength('nome', 100)
            ->requirePresence('nome', 'create')
            ->notEmptyString('nome');

        $validator
            ->boolean('is_aplicacao')
            ->requirePresence('is_aplicacao', 'create')
            ->notEmptyString('is_aplicacao');

        return $validator;
    }
}
