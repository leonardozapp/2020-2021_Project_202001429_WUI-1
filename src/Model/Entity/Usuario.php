<?php

declare(strict_types=1);

namespace App\Model\Entity;

use Cake\ORM\Entity;

/**
 * Usuario Entity
 *
 * @property int $id
 * @property string $cpf
 * @property string $nome
 * @property string $email
 * @property \Cake\I18n\FrozenDate $dt_nasc
 * @property string $senha
 * @property \Cake\I18n\FrozenDate $dt_reg
 * @property int $tipo_plano_id
 * @property int $tipo_etapas_registro_id
 * @property string|null $coment
 * @property \Cake\I18n\FrozenTime $created
 * @property \Cake\I18n\FrozenTime|null $modified
 *
 * @property \App\Model\Entity\TipoPlano $tipo_plano
 * @property \App\Model\Entity\TipoEtapasRegistro $tipo_etapas_registro
 * @property \App\Model\Entity\CarteirasInvestimento[] $carteiras_investimentos
 * @property \App\Model\Entity\TipoPerfilInvestidor $tipo_perfil_investidor
 */
class Usuario extends Entity
{
    /**
     * Fields that can be mass assigned using newEntity() or patchEntity().
     *
     * Note that when '*' is set to true, this allows all unspecified fields to
     * be mass assigned. For security purposes, it is advised to set '*' to false
     * (or remove it), and explicitly make individual fields accessible as needed.
     *
     * @var array
     */
    protected $_accessible = [
        'cpf' => true,
        'nome' => true,
        'email' => true,
        'dt_nasc' => true,
        'senha' => true,
        'dt_reg' => true,
        'tipo_plano_id' => true,
        'tipo_etapas_registro_id' => true,
        'coment' => true,
        'created' => true,
        'modified' => true,
        'tipo_plano' => true,
        'tipo_etapas_registro' => true,
        'carteiras_investimentos' => true,
        'tipo_perfil_investidor' => true,
    ];
}
