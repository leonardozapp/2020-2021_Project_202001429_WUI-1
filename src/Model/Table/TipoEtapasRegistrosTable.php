<?php
declare(strict_types=1);

namespace App\Model\Table;

use Cake\ORM\Query;
use Cake\ORM\RulesChecker;
use Cake\ORM\Table;
use Cake\Validation\Validator;

/**
 * TipoEtapasRegistros Model
 *
 * @property \App\Model\Table\UsuariosTable&\Cake\ORM\Association\HasMany $Usuarios
 *
 * @method \App\Model\Entity\TipoEtapasRegistro newEmptyEntity()
 * @method \App\Model\Entity\TipoEtapasRegistro newEntity(array $data, array $options = [])
 * @method \App\Model\Entity\TipoEtapasRegistro[] newEntities(array $data, array $options = [])
 * @method \App\Model\Entity\TipoEtapasRegistro get($primaryKey, $options = [])
 * @method \App\Model\Entity\TipoEtapasRegistro findOrCreate($search, ?callable $callback = null, $options = [])
 * @method \App\Model\Entity\TipoEtapasRegistro patchEntity(\Cake\Datasource\EntityInterface $entity, array $data, array $options = [])
 * @method \App\Model\Entity\TipoEtapasRegistro[] patchEntities(iterable $entities, array $data, array $options = [])
 * @method \App\Model\Entity\TipoEtapasRegistro|false save(\Cake\Datasource\EntityInterface $entity, $options = [])
 * @method \App\Model\Entity\TipoEtapasRegistro saveOrFail(\Cake\Datasource\EntityInterface $entity, $options = [])
 * @method \App\Model\Entity\TipoEtapasRegistro[]|\Cake\Datasource\ResultSetInterface|false saveMany(iterable $entities, $options = [])
 * @method \App\Model\Entity\TipoEtapasRegistro[]|\Cake\Datasource\ResultSetInterface saveManyOrFail(iterable $entities, $options = [])
 * @method \App\Model\Entity\TipoEtapasRegistro[]|\Cake\Datasource\ResultSetInterface|false deleteMany(iterable $entities, $options = [])
 * @method \App\Model\Entity\TipoEtapasRegistro[]|\Cake\Datasource\ResultSetInterface deleteManyOrFail(iterable $entities, $options = [])
 */
class TipoEtapasRegistrosTable extends Table
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

        $this->setTable('tipo_etapas_registros');
        $this->setDisplayField('etapa');
        $this->setPrimaryKey('id');

        $this->hasMany('Usuarios', [
            'foreignKey' => 'tipo_etapas_registro_id',
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
            ->integer('fase')
            ->requirePresence('fase', 'create')
            ->notEmptyString('fase');

        $validator
            ->scalar('etapa')
            ->maxLength('etapa', 100)
            ->requirePresence('etapa', 'create')
            ->notEmptyString('etapa');

        $validator
            ->boolean('podeLogar')
            ->requirePresence('podeLogar', 'create')
            ->notEmptyString('podeLogar');

        $validator
            ->boolean('podeInvestir')
            ->requirePresence('podeInvestir', 'create')
            ->notEmptyString('podeInvestir');

        return $validator;
    }
}
