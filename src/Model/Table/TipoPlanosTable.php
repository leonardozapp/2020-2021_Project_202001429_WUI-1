<?php
declare(strict_types=1);

namespace App\Model\Table;

use Cake\ORM\Query;
use Cake\ORM\RulesChecker;
use Cake\ORM\Table;
use Cake\Validation\Validator;

/**
 * TipoPlanos Model
 *
 * @property \App\Model\Table\PermissaosTable&\Cake\ORM\Association\BelongsTo $Permissaos
 * @property \App\Model\Table\UsuariosTable&\Cake\ORM\Association\HasMany $Usuarios
 *
 * @method \App\Model\Entity\TipoPlano newEmptyEntity()
 * @method \App\Model\Entity\TipoPlano newEntity(array $data, array $options = [])
 * @method \App\Model\Entity\TipoPlano[] newEntities(array $data, array $options = [])
 * @method \App\Model\Entity\TipoPlano get($primaryKey, $options = [])
 * @method \App\Model\Entity\TipoPlano findOrCreate($search, ?callable $callback = null, $options = [])
 * @method \App\Model\Entity\TipoPlano patchEntity(\Cake\Datasource\EntityInterface $entity, array $data, array $options = [])
 * @method \App\Model\Entity\TipoPlano[] patchEntities(iterable $entities, array $data, array $options = [])
 * @method \App\Model\Entity\TipoPlano|false save(\Cake\Datasource\EntityInterface $entity, $options = [])
 * @method \App\Model\Entity\TipoPlano saveOrFail(\Cake\Datasource\EntityInterface $entity, $options = [])
 * @method \App\Model\Entity\TipoPlano[]|\Cake\Datasource\ResultSetInterface|false saveMany(iterable $entities, $options = [])
 * @method \App\Model\Entity\TipoPlano[]|\Cake\Datasource\ResultSetInterface saveManyOrFail(iterable $entities, $options = [])
 * @method \App\Model\Entity\TipoPlano[]|\Cake\Datasource\ResultSetInterface|false deleteMany(iterable $entities, $options = [])
 * @method \App\Model\Entity\TipoPlano[]|\Cake\Datasource\ResultSetInterface deleteManyOrFail(iterable $entities, $options = [])
 */
class TipoPlanosTable extends Table
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

        $this->setTable('tipo_planos');
        $this->setDisplayField('nome');
        $this->setPrimaryKey('id');

        $this->belongsTo('Permissaos', [
            'foreignKey' => 'permissao_id',
            'joinType' => 'INNER',
        ]);
        $this->hasMany('Usuarios', [
            'foreignKey' => 'tipo_plano_id',
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
            ->maxLength('nome', 40)
            ->requirePresence('nome', 'create')
            ->notEmptyString('nome');

        $validator
            ->scalar('descricao')
            ->maxLength('descricao', 200)
            ->requirePresence('descricao', 'create')
            ->notEmptyString('descricao');

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
        $rules->add($rules->existsIn(['permissao_id'], 'Permissaos'));

        return $rules;
    }
}
